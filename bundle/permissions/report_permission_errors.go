package permissions

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/log"
)

// We recognize many different kinds of permission errors.
// They are assigned different error codes to help support customers
// and potentially aid in future tooling support.
const ErrorCannotChangePathPermissions = "EPERM1"
const ErrorPathAccessDenied = "EPERM2"
const ErrorCannotChangeResourcePermissions = "EPERM3"
const ErrorResourceAccessDenied = "EPERM4"
const ErrorRunAsDenied = "EPERM5"

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
	if len(b.Config.Permissions) > 0 && !canManageBundle {
		return diag.Diagnostics{
			{
				Severity: diag.Warning,
				Summary:  fmt.Sprintf("Permissions section should list %s or one of their groups with CAN_MANAGE permissions", b.Config.Workspace.CurrentUser.UserName),
				Location: b.Config.GetLocation("permissions"),
			},
		}
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
	if b.Config.RunAs != nil && b.Config.RunAs.UserName != "" {
		otherManagers = append(otherManagers, b.Config.RunAs.UserName)
	}

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
			otherManager = p.GroupName
		}
		if otherManager == "" {
			// Skip service principals
			continue
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

func runsAsCurrentUser(b *bundle.Bundle) bool {
	user := b.Config.Workspace.CurrentUser.UserName
	runAs := b.Config.RunAs
	return runAs == nil || runAs.UserName == user || runAs.ServicePrincipalName == user
}

func ReportPermissionDenied(ctx context.Context, b *bundle.Bundle, path string) diag.Diagnostics {
	log.Errorf(ctx, "Failed to update %v", path)

	user := b.Config.Workspace.CurrentUser.UserName
	_, otherManagers := analyzeBundlePermissions(b)
	assistance := fmt.Sprintf("For assistance, users or groups who may be able to update the permissions include: %s.", otherManagers)
	if otherManagers == "" {
		assistance = "For assistance, contact the owners of this project."
	}

	if hasPermissionAccordingToBundle(b) {
		// According databricks.yml, the current user has the right permissions.
		// But we're still seeing permission errors. So someone else will need
		// to redeploy the bundle with the right set of permissions.
		return diag.Errorf("permission error [%s]: access denied updating deployment permissions for %s.\n"+
			"%s\n"+
			"They can redeploy the project to apply the latest set of permissions.\n"+
			"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
			ErrorCannotChangePathPermissions, user, assistance)
	}

	return diag.Errorf("permission error [%s]: %s doesn't have the necessary permissions to deploy.\n"+
		"%s\n"+
		"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
		ErrorPathAccessDenied, user, assistance)

}

func TryReportTerraformPermissionError(ctx context.Context, b *bundle.Bundle, err error) diag.Diagnostics {
	_, otherManagers := analyzeBundlePermissions(b)
	assistance := fmt.Sprintf("For assistance, users or groups who may be able to update the permissions include: %s.", otherManagers)
	if otherManagers == "" {
		assistance = "For assistance, contact the owners of this project."
	}

	if !strings.Contains(err.Error(), "cannot update permissions") && !strings.Contains(err.Error(), "permissions on pipeline") && !strings.Contains(err.Error(), "cannot read permissions") {
		return nil
	}

	log.Errorf(ctx, "Terraform error during deployment: %v", err.Error())

	// Best-effort attempt to extract the resource name from the error message.
	re := regexp.MustCompile(`databricks_(\w*)\.(\w*)`)
	match := re.FindStringSubmatch(err.Error())
	resource := "resource"
	if len(match) > 1 {
		resource = match[2]
	}

	if runsAsCurrentUser(b) {
		return diag.Errorf("permission error [%s]: access denied updating permissions to %s.\n"+
			"Redeploying resources with another owner or run_as identity is currently not supported.\n"+
			"%s\n"+
			"Only the current owner of the resource or a workspace admin can redeploy this resource.\n"+
			"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
			ErrorCannotChangeResourcePermissions, resource, assistance)
	}
	return diag.Errorf("permission error [%s]: access denied updating permissions to %s.\n"+
		"%s\n"+
		"They can redeploy the project to apply the latest set of permissions.\n"+
		"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
		ErrorResourceAccessDenied, resource, assistance)
}
