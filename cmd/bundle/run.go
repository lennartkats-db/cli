package bundle

import (
	"encoding/json"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/run"
	"github.com/databricks/cli/cmd/root"
	"github.com/databricks/cli/libs/flags"
	"github.com/spf13/cobra"
)

func newRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run [flags] KEY",
		Short: "Run a resource (e.g. a job or a pipeline)",

		Args:    cobra.MaximumNArgs(1),
		PreRunE: ConfigureBundleWithVariables,
	}

	var runOptions run.Options
	runOptions.Define(cmd.Flags())

	var noWait bool
	cmd.Flags().BoolVar(&noWait, "no-wait", false, "Don't wait for the run to complete.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		run.GetRunnerForCommand(ctx, cmd, true)

		runOptions.NoWait = noWait
		output, err := runner.Run(ctx, &runOptions)
		if err != nil {
			return err
		}
		if output != nil {
			switch root.OutputType(cmd) {
			case flags.OutputText:
				resultString, err := output.String()
				if err != nil {
					return err
				}
				cmd.OutOrStdout().Write([]byte(resultString))
			case flags.OutputJSON:
				b, err := json.MarshalIndent(output, "", "  ")
				if err != nil {
					return err
				}
				cmd.OutOrStdout().Write(b)
			default:
				return fmt.Errorf("unknown output type %s", root.OutputType(cmd))
			}
		}
		return nil
	}

	cmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		err := root.MustConfigureBundle(cmd, args)
		if err != nil {
			cobra.CompErrorln(err.Error())
			return nil, cobra.ShellCompDirectiveError
		}

		// No completion in the context of a bundle.
		// Source and destination paths are taken from bundle configuration.
		b := bundle.GetOrNil(cmd.Context())
		if b == nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		return run.ResourceCompletions(b), cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}
