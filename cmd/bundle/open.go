package bundle

import (
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/deploy/terraform"
	"github.com/databricks/cli/bundle/phases"
	"github.com/databricks/cli/bundle/run"
	"github.com/databricks/cli/libs/cmdio"
	"github.com/spf13/cobra"
)

func newOpenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "open KEY",
		Short:   "Open a resource in the browser (e.g. a job or pipeline)",
		Args:    cobra.MaximumNArgs(1),
		PreRunE: ConfigureBundleWithVariables,
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		b := bundle.Get(cmd.Context())

		err := bundle.Apply(ctx, b, bundle.Seq(
			phases.Initialize(),
			terraform.Interpolate(),
			terraform.Write(),
			terraform.StatePull(),
			terraform.Load(terraform.ErrorOnEmptyState),
		))
		if err != nil {
			return err
		}

		// If no arguments are specified, prompt the user to select something to run.
		if len(args) == 0 && cmdio.IsInteractive(ctx) {
			// Invert completions from KEY -> NAME, to NAME -> KEY.
			inv := make(map[string]string)
			for k, v := range run.ResourceCompletionMap(b) {
				inv[v] = k
			}
			id, err := cmdio.Select(ctx, inv, "Resource to run")
			if err != nil {
				return err
			}
			args = append(args, id)
		}

		if len(args) != 1 {
			return fmt.Errorf("expected a KEY of the resource to run")
		}

		cmdio.LogString(ctx, "Opening resource in browser...")
		return nil
	}

	return cmd
}
