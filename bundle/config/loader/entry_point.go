package loader

import (
	"context"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/libs/diag"
)

type entryPoint struct{}

// EntryPoint loads the entry point configuration.
func EntryPoint() bundle.Mutator {
	return &entryPoint{}
}

func (m *entryPoint) Name() string {
	return "EntryPoint"
}

func (m *entryPoint) Apply(_ context.Context, b *bundle.Bundle) diag.Diagnostics {
	path, err := config.FileNames.FindInPath(b.RootPath)
	if err != nil {
		return diag.FromErr(diag.ConfigurationError, err)
	}
	this, diags := config.Load(path)
	if diags.HasError() {
		return diags
	}
	err = b.Config.Merge(this)
	if err != nil {
		diags = diags.Extend(diag.FromErr(diag.ConfigurationError, err))
	}
	return diags
}
