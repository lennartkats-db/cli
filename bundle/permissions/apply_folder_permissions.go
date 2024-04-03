package permissions

import (
	"context"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

type applyFolderPermissions struct {
}

func ApplyFolderPermissions() bundle.Mutator {
	return &applyFolderPermissions{}
}

// Apply implements bundle.Mutator.
func (*applyFolderPermissions) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	err := giveAccessForWorkspaceRoot(ctx, b)
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func (*applyFolderPermissions) Name() string {
	return "ApplyFolderPermissions"
}

func giveAccessForWorkspaceRoot(ctx context.Context, b *bundle.Bundle) error {
	permissions := make([]workspace.WorkspaceObjectAccessControlRequest, 0)

	for _, p := range b.Config.Permissions {
		level, err := getWorkspaceObjectPermissionLevel(p.Level)
		if err != nil {
			return err
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
		return err
	}

	_, err = w.SetPermissions(ctx, workspace.WorkspaceObjectPermissionsRequest{
		WorkspaceObjectId:   fmt.Sprint(obj.ObjectId),
		WorkspaceObjectType: "directories",
		AccessControlList:   permissions,
	})
	return err
}

func getWorkspaceObjectPermissionLevel(bundlePermission string) (workspace.WorkspaceObjectPermissionLevel, error) {
	switch bundlePermission {
	case CAN_MANAGE, IS_OWNER:
		return workspace.WorkspaceObjectPermissionLevelCanManage, nil
	case CAN_RUN:
		return workspace.WorkspaceObjectPermissionLevelCanRun, nil
	case CAN_VIEW:
		return workspace.WorkspaceObjectPermissionLevelCanRead, nil
	default:
		return "", fmt.Errorf("unsupported bundle permission level %s", bundlePermission)
	}
}
