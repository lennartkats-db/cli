package terraform

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/diag"
	"github.com/hashicorp/terraform-exec/tfexec"
)

type BindOptions struct {
	AutoApprove  bool
	ResourceType string
	ResourceKey  string
	ResourceId   string
}

type importResource struct {
	opts *BindOptions
}

// Apply implements bundle.Mutator.
func (m *importResource) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	dir, err := Dir(ctx, b)
	if err != nil {
		return diag.FromErr(diag.IOError, err)
	}

	tf := b.Terraform
	if tf == nil {
		return diag.Errorf(diag.InternalError)("terraform not initialized")
	}

	err = tf.Init(ctx, tfexec.Upgrade(true))
	if err != nil {
		return diag.Errorf(diag.TerraformSetupError)("terraform init: %v", err)
	}
	tmpDir, err := os.MkdirTemp("", "state-*")
	if err != nil {
		return diag.Errorf(diag.IOError)("temp dir: %v", err)
	}
	tmpState := filepath.Join(tmpDir, TerraformStateFileName)

	importAddress := fmt.Sprintf("%s.%s", m.opts.ResourceType, m.opts.ResourceKey)
	err = tf.Import(ctx, importAddress, m.opts.ResourceId, tfexec.StateOut(tmpState))
	if err != nil {
		return diag.Errorf(diag.TerraformError)("terraform import: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	tf.SetStdout(buf)

	//lint:ignore SA1019 We use legacy -state flag for now to plan the import changes based on temporary state file
	changed, err := tf.Plan(ctx, tfexec.State(tmpState), tfexec.Target(importAddress))
	if err != nil {
		return diag.Errorf(diag.TerraformError)("terraform plan: %v", err)
	}

	defer os.RemoveAll(tmpDir)

	if changed && !m.opts.AutoApprove {
		output := buf.String()
		// Remove output starting from Warning until end of output
		output = output[:bytes.Index([]byte(output), []byte("Warning:"))]
		cmdio.LogString(ctx, output)
		ans, err := cmdio.AskYesOrNo(ctx, "Confirm import changes? Changes will be remotely applied only after running 'bundle deploy'.")
		if err != nil {
			return diag.FromErr(diag.IOError, err)
		}
		if !ans {
			return diag.Errorf(diag.AbortedError)("import aborted")
		}
	}

	// If user confirmed changes, move the state file from temp dir to state location
	f, err := os.Create(filepath.Join(dir, TerraformStateFileName))
	if err != nil {
		return diag.FromErr(diag.IOError, err)
	}
	defer f.Close()

	tmpF, err := os.Open(tmpState)
	if err != nil {
		return diag.FromErr(diag.IOError, err)
	}
	defer tmpF.Close()

	_, err = io.Copy(f, tmpF)
	if err != nil {
		return diag.FromErr(diag.IOError, err)
	}

	return nil
}

// Name implements bundle.Mutator.
func (*importResource) Name() string {
	return "terraform.Import"
}

func Import(opts *BindOptions) bundle.Mutator {
	return &importResource{opts: opts}
}
