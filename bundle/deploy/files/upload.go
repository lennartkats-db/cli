package files

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/permissions"
	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/log"
)

type upload struct{}

func (m *upload) Name() string {
	return "files.Upload"
}

func (m *upload) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	cmdio.LogString(ctx, fmt.Sprintf("Uploading bundle files to %s...", b.Config.Workspace.FilePath))
	sync, err := GetSync(ctx, bundle.ReadOnly(b))
	if err != nil {
		return diag.FromErr(diag.SyncError, err)
	}

	b.Files, err = sync.RunOnce(ctx)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return permissions.ReportPermissionDenied(ctx, b, b.Config.Workspace.StatePath)
		}
		return diag.FromErr(diag.SyncError, err)
	}

	log.Infof(ctx, "Uploaded bundle files")
	return nil
}

func Upload() bundle.Mutator {
	return &upload{}
}
