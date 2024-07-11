package files

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/sync"
	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/fatih/color"
)

type delete struct{}

func (m *delete) Name() string {
	return "files.Delete"
}

func (m *delete) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	// Do not delete files if terraform destroy was not consented
	if !b.Plan.IsEmpty && !b.Plan.ConfirmApply {
		return nil
	}

	cmdio.LogString(ctx, "Starting deletion of remote bundle files")
	cmdio.LogString(ctx, fmt.Sprintf("Bundle remote directory is %s", b.Config.Workspace.RootPath))

	red := color.New(color.FgRed).SprintFunc()
	if !b.AutoApprove {
		proceed, err := cmdio.AskYesOrNo(ctx, fmt.Sprintf("\n%s and all files in it will be %s Proceed?", b.Config.Workspace.RootPath, red("deleted permanently!")))
		if err != nil {
			return diag.FromErr(diag.AbortedError, err)
		}
		if !proceed {
			return nil
		}
	}

	err := b.WorkspaceClient().Workspace.Delete(ctx, workspace.Delete{
		Path:      b.Config.Workspace.RootPath,
		Recursive: true,
	})
	if err != nil {
		return diag.FromErr(diag.WorkspaceClientError, err)
	}

	// Clean up sync snapshot file
	err = deleteSnapshotFile(ctx, b)
	if err != nil {
		return diag.FromErr(diag.WorkspaceClientError, err)
	}

	cmdio.LogString(ctx, "Successfully deleted files!")
	return nil
}

func deleteSnapshotFile(ctx context.Context, b *bundle.Bundle) error {
	opts, err := GetSyncOptions(ctx, bundle.ReadOnly(b))
	if err != nil {
		return fmt.Errorf("cannot get sync options: %w", err)
	}
	sp, err := sync.SnapshotPath(opts)
	if err != nil {
		return err
	}
	err = os.Remove(sp)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to destroy sync snapshot file: %s", err)
	}
	return nil
}

func Delete() bundle.Mutator {
	return &delete{}
}
