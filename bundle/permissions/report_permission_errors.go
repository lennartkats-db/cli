package permissions

import (
	"context"
	"regexp"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/log"
)

const CheckPermissionsFilename = "permissions.check"

type reportPermissionErrors struct{}

func ReportPermissionErrors() bundle.Mutator {
	return &reportPermissionErrors{}
}

func (m *reportPermissionErrors) Name() string {
	return "CheckPermissions"
}

func (m *reportPermissionErrors) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	canManageBundle, _ := analyzeBundlePermissions(b)
	if !canManageBundle {
		return diag.Errorf("User %s doesn't have the necessary permissions to deploy.\nPlease make sure the current user has CAN_MANAGE permissions in the permissions section of databricks.yml.\nSee also https://docs.databricks.com/en/dev-tools/bundles/permissions.html.", b.Config.Workspace.CurrentUser.UserName)
	}

	return nil
}

func hasPermissionAccordingToBundle(b *bundle.Bundle) bool {
	currentUserGroups := b.Config.Workspace.CurrentUser.User.Groups
	targetPermissions := b.Config.Permissions
	for _, p := range targetPermissions {
		if p.Level != "CAN_MANAGE" {
			continue
		}
		if p.UserName == b.Config.Workspace.CurrentUser.UserName {
			return true
		}
		for _, group := range currentUserGroups {
			if p.GroupName == group.Display {
				return true
			}
		}
	}
	return false
}

// analyzeBundlePermissions analyzes the top-level permissions of the bundle.
// This permission set is important since it determines the permissions of the
// target workspace folder.
//
// Returns:
// - isManager: true if the current user is can manage the bundle resources.
// - otherManagers: a list of other managers of the bundle resources.
func analyzeBundlePermissions(b *bundle.Bundle) (bool, string) {
	isManager := false
	otherManagers := []string{}

	currentUser := b.Config.Workspace.CurrentUser.UserName
	targetPermissions := b.Config.Permissions
	for _, p := range targetPermissions {
		if p.Level != "CAN_MANAGE" {
			continue
		}

		if p.UserName == currentUser || p.ServicePrincipalName == currentUser {
			isManager = true
			continue
		}

		if isGroupOfCurrentUser(b, p.GroupName) {
			isManager = true
			continue
		}

		// Permission doesn't apply to current user; add to otherManagers
		otherManager := p.UserName
		if otherManager == "" {
			otherManager = p.ServicePrincipalName
		}
		if otherManager == "" {
			otherManager = p.GroupName
		}
		otherManagers = append(otherManagers, otherManager)
	}
	return isManager, strings.Join(otherManagers, ", ")
}

func isGroupOfCurrentUser(b *bundle.Bundle, groupName string) bool {
	currentUserGroups := b.Config.Workspace.CurrentUser.User.Groups

	for _, g := range currentUserGroups {
		if g.Display == groupName {
			return true
		}
	}
	return false
}

// func runsAsCurrentUser(b *bundle.Bundle) bool {
// 	user := b.Config.Workspace.CurrentUser.UserName
// 	runAs := b.Config.RunAs
// 	return runAs == nil || runAs.UserName == user || runAs.ServicePrincipalName == user
// }

func ReportPermissionDenied(ctx context.Context, b *bundle.Bundle, path string) diag.Diagnostics {
	log.Errorf(ctx, "Failed to update %v", path)

	user := b.Config.Workspace.CurrentUser.UserName
	_, otherManagers := analyzeBundlePermissions(b)

	if hasPermissionAccordingToBundle(b) {
		// According databricks.yml, the current user has the right permissions.
		// But we're still seeing permission errors. So someone else will need
		// to redeploy the bundle with the right set of permissions.
		return diag.Errorf("permission error [EPERM1]: access denied to update permissions for %s.\n"+
			"For assistance, users or groups who may be able to update the permissions include: %s.\n"+
			"They can redeploy the project to apply the latest set of permissions.\n"+
			"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
			user, otherManagers)
	}

	return diag.Errorf("permission error [EPERM2]: %s doesn't have the necessary permissions to deploy.\n"+
		"For assistance, users or groups who may be able to update the permissions include: %s.\n"+
		"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
		user, otherManagers)

}

func TryReportTerraformPermissionError(b *bundle.Bundle, err error) diag.Diagnostics {
	_, otherManagers := analyzeBundlePermissions(b)

	if strings.Contains(err.Error(), "cannot update permissions") || strings.Contains(err.Error(), "permissions on pipeline") {
		// Best-effort attempt to extract the resource name from the error message.
		re := regexp.MustCompile(`databricks_(\w*)\.[^-]*-(\w*)`)
		match := re.FindStringSubmatch(err.Error())
		resourceName := "resource"

		if len(match) > 1 {
			resourceName = match[2]
			resource, err := b.Config.Resources.FindResourceByConfigKey(resourceName)
			if err == nil && !resource.IsOwnerChangeSupported() {
				return diag.Errorf("permission error [EPERM3]: unable change permissions of %s.\n"+
					"For this resource type, only deployment by the current owner of the resource or a workspace admin is supported.\n"+
					"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
					resourceName)
			}
		}
		return diag.Errorf("permission error [EPERM4]: access denied updating %s.\n"+
			"For assistance, users or groups who may be able to update the permissions include: %s.\n"+
			"They can redeploy the project to apply the latest set of permissions.\n"+
			"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
			resourceName, otherManagers)
	}

	return nil
}
