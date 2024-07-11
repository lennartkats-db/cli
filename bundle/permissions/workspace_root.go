package permissions

import (
	"context"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

type workspaceRootPermissions struct {
}

func ApplyWorkspaceRootPermissions() bundle.Mutator {
	return &workspaceRootPermissions{}
}

// Apply implements bundle.Mutator.
func (*workspaceRootPermissions) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	return giveAccessForWorkspaceRoot(ctx, b)
}

func (*workspaceRootPermissions) Name() string {
	return "ApplyWorkspaceRootPermissions"
}

func giveAccessForWorkspaceRoot(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	permissions := make([]workspace.WorkspaceObjectAccessControlRequest, 0)

	for _, p := range b.Config.Permissions {
		level, err := getWorkspaceObjectPermissionLevel(p.Level)
		if err != nil {
			return diag.FromErr(diag.ConfigurationError, err)
		}

		permissions = append(permissions, workspace.WorkspaceObjectAccessControlRequest{
			GroupName:            p.GroupName,
			UserName:             p.UserName,
			ServicePrincipalName: p.ServicePrincipalName,
			PermissionLevel:      level,
		})
	}

	if len(permissions) == 0 {
		return nil
	}

	w := b.WorkspaceClient().Workspace
	obj, err := w.GetStatusByPath(ctx, b.Config.Workspace.RootPath)
	if err != nil {
		return diag.FromErr(diag.WorkspaceClientError, err)
	}

	_, err = w.UpdatePermissions(ctx, workspace.WorkspaceObjectPermissionsRequest{
		WorkspaceObjectId:   fmt.Sprint(obj.ObjectId),
		WorkspaceObjectType: "directories",
		AccessControlList:   permissions,
	})
	return diag.FromErr(diag.WorkspaceClientError, err)
}

func getWorkspaceObjectPermissionLevel(bundlePermission string) (workspace.WorkspaceObjectPermissionLevel, error) {
	switch bundlePermission {
	case CAN_MANAGE:
		return workspace.WorkspaceObjectPermissionLevelCanManage, nil
	case CAN_RUN:
		return workspace.WorkspaceObjectPermissionLevelCanRun, nil
	case CAN_VIEW:
		return workspace.WorkspaceObjectPermissionLevelCanRead, nil
	default:
		return "", fmt.Errorf("unsupported bundle permission level %s", bundlePermission)
	}
}
