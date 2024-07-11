package mutator

import (
	"context"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/env"
	"github.com/databricks/cli/libs/filer"
	"github.com/databricks/cli/libs/vfs"
)

const envDatabricksRuntimeVersion = "DATABRICKS_RUNTIME_VERSION"

type configureWSFS struct{}

func ConfigureWSFS() bundle.Mutator {
	return &configureWSFS{}
}

func (m *configureWSFS) Name() string {
	return "ConfigureWSFS"
}

func (m *configureWSFS) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	root := b.BundleRoot.Native()

	// The bundle root must be located in /Workspace/
	if !strings.HasPrefix(root, "/Workspace/") {
		return nil
	}

	// The executable must be running on DBR.
	if _, ok := env.Lookup(ctx, envDatabricksRuntimeVersion); !ok {
		return nil
	}

	// If so, swap out vfs.Path instance of the sync root with one that
	// makes all Workspace File System interactions extension aware.
	p, err := vfs.NewFilerPath(ctx, root, func(path string) (filer.Filer, error) {
		return filer.NewWorkspaceFilesExtensionsClient(b.WorkspaceClient(), path)
	})
	if err != nil {
		return diag.FromErr(diag.IOError, err)
	}

	b.BundleRoot = p
	return nil
}
