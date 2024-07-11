package terraform

import (
	"context"
	"fmt"
	"slices"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/hashicorp/terraform-exec/tfexec"
)

type loadMode int

const ErrorOnEmptyState loadMode = 0

type load struct {
	modes []loadMode
}

func (l *load) Name() string {
	return "terraform.Load"
}

func (l *load) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	tf := b.Terraform
	if tf == nil {
		return diag.Errorf(diag.InternalError)("terraform not initialized")
	}

	err := tf.Init(ctx, tfexec.Upgrade(true))
	if err != nil {
		return diag.Errorf(diag.TerraformSetupError)("terraform init: %v", err)
	}

	state, err := ParseResourcesState(ctx, b)
	if err != nil {
		return diag.FromErr(diag.TerraformError, err)
	}

	err = l.validateState(state)
	if err != nil {
		return diag.FromErr(diag.TerraformError, err)
	}

	// Merge state into configuration.
	err = TerraformToBundle(state, &b.Config)
	if err != nil {
		return diag.FromErr(diag.InternalError, err)
	}

	return nil
}

func (l *load) validateState(state *resourcesState) error {
	if state.Version != SupportedStateVersion {
		return fmt.Errorf("unsupported deployment state version: %d. Try re-deploying the bundle", state.Version)
	}

	if len(state.Resources) == 0 && slices.Contains(l.modes, ErrorOnEmptyState) {
		return fmt.Errorf("no deployment state. Did you forget to run 'databricks bundle deploy'?")
	}

	return nil
}

func Load(modes ...loadMode) bundle.Mutator {
	return &load{modes: modes}
}
