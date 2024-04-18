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
		if b.Config.Experimental == nil || !b.Config.Experimental.NewPermissionModel {
			// Just show a warning
			return diag.Diagnostics{{
				Severity: diag.Warning,
				Summary:  fmt.Sprintf("permissions section should include %s or one of their groups with CAN_MANAGE permissions", b.Config.Workspace.CurrentUser.UserName),
				Location: b.Config.GetLocation("permissions"),
				ID:       diag.PermissionNotIncluded,
			}}
		}

		// Show an error for this state. If the bundle has a permissions section,
		// but it only lists other users, then we will likely get an error during deployment.
		// Better fail fast and encourage the user to be explicit!
		return diag.Diagnostics{{
			Severity: diag.Error,
			Summary:  fmt.Sprintf("permissions section must include %s or one of their groups with CAN_MANAGE permissions", b.Config.Workspace.CurrentUser.UserName),
			Location: b.Config.GetLocation("permissions"),
			ID:       diag.PermissionNotIncluded,
		}}
	}

	return nil
}

// analyzeBundlePermissions analyzes the top-level permissions of the bundle.
// This permission set is important since it determines the permissions of the
// target workspace folder.
//
// Returns:
// - isManager: true if the current user is can manage the bundle resources.
// - assistance: advice on who to contact as to manage this project
func analyzeBundlePermissions(b *bundle.Bundle) (bool, string) {
	canManageBundle := false
	otherManagers := make(map[string]bool)
	if b.Config.RunAs != nil && b.Config.RunAs.UserName != "" {
		// The run_as user is another human that could be contacted
		// about this bundle.
		otherManagers[b.Config.RunAs.UserName] = true
	}

	currentUser := b.Config.Workspace.CurrentUser.UserName
	targetPermissions := b.Config.Permissions
	for _, p := range targetPermissions {
		if p.Level != "CAN_MANAGE" {
			continue
		}

		if p.UserName == currentUser || p.ServicePrincipalName == currentUser {
			canManageBundle = true
			continue
		}

		if isGroupOfCurrentUser(b, p.GroupName) {
			canManageBundle = true
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
		otherManagers[otherManager] = true
	}

	var managersSlice []string
	for manager := range otherManagers {
		managersSlice = append(managersSlice, manager)
	}

	assistance := "For assistance, contact the owners of this project."
	if len(managersSlice) > 0 {
		assistance = fmt.Sprintf("For assistance, users or groups who may be able to update the permissions include: %s.", strings.Join(managersSlice, ", "))
	}
	return canManageBundle, assistance
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
	canManageBundle, assistance := analyzeBundlePermissions(b)

	if !canManageBundle {
		return diag.Diagnostics{{
			Summary: fmt.Sprintf("deployment access denied for %s.\n"+
				"Please make sure the current user or one of their groups is listed under the permissions of this bundle.\n"+
				"%s\n"+
				"They may need to redeploy the bundle to apply the new permissions.\n"+
				"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
				user, assistance),
			Severity: diag.Error,
			ID:       diag.PermissionNotIncluded,
		}}
	}

	// According databricks.yml, the current user has the right permissions.
	// But we're still seeing permission errors. So someone else will need
	// to redeploy the bundle with the right set of permissions.
	return diag.Diagnostics{{
		Summary: fmt.Sprintf("access denied updating deployment permissions for %s.\n"+
			"%s\n"+
			"They can redeploy the project to apply the latest set of permissions.\n"+
			"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
			user, assistance),
		Severity: diag.Error,
		ID:       diag.CannotChangePathPermissions,
	}}
}

func TryReportTerraformPermissionError(ctx context.Context, b *bundle.Bundle, err error) diag.Diagnostics {
	_, assistance := analyzeBundlePermissions(b)

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
		return diag.Diagnostics{{
			Summary: fmt.Sprintf("access denied updating permissions to %s.\n"+
				"Redeploying resources with another owner or run_as identity is currently not supported.\n"+
				"%s\n"+
				"Only the current owner of the resource or a workspace admin can redeploy this resource.\n"+
				"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
				resource, assistance),
			Severity: diag.Error,
			ID:       diag.CannotChangeResourcePermissions,
		}}
	}
	return diag.Diagnostics{{
		Summary: fmt.Sprintf("access denied updating permissions to %s.\n"+
			"%s\n"+
			"They can redeploy the project to apply the latest set of permissions.\n"+
			"Please refer to https://docs.databricks.com/en/dev-tools/bundles/permissions.html for more on managing permissions.",
			resource, assistance),
		Severity: diag.Error,
		ID:       diag.ResourceAccessDenied,
	}}
}
