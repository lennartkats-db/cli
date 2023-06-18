package bundle

import (
	"encoding/json"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/deploy/terraform"
	"github.com/databricks/cli/bundle/phases"
	"github.com/databricks/cli/bundle/run"
	"github.com/databricks/cli/cmd/root"
	"github.com/databricks/cli/libs/flags"
	"github.com/spf13/cobra"
)

var runOptions run.Options
var deploy bool
var noWait bool

var runCmd = &cobra.Command{
	Use:   "run [flags] KEY",
	Short: "Run a workload (e.g. a job or a pipeline)",

	Args:    cobra.ExactArgs(1),
	PreRunE: ConfigureBundleWithVariables,
	RunE: func(cmd *cobra.Command, args []string) error {
		b := bundle.Get(cmd.Context())

		if deploy {
			b.Config.Bundle.Lock.Force = force
			err := bundle.Apply(cmd.Context(), b, bundle.Seq(
				phases.Initialize(computeID),
				phases.Build(),
				phases.Deploy(),
			))
			if err != nil {
				return err
			}
		} else if computeID != "" {
			// Running notebooks is not yet implemented, otherwise we could
			// use --compute with a notebook
			return fmt.Errorf("not supported: --compute specified without --deploy")
		}

		err := bundle.Apply(cmd.Context(), b, bundle.Seq(
			phases.Initialize(computeID),
			terraform.Interpolate(),
			terraform.Write(),
			terraform.StatePull(),
			terraform.Load(),
		))
		if err != nil {
			return err
		}

		runner, err := run.Find(b, args[0])
		if err != nil {
			return err
		}

		runOptions.NoWait = noWait
		output, err := runner.Run(cmd.Context(), &runOptions)
		if err != nil {
			return err
		}
		if output != nil {
			switch root.OutputType() {
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
				return fmt.Errorf("unknown output type %s", root.OutputType())
			}
		}
		return nil
	},

	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
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
	},
}

func init() {
	runOptions.Define(runCmd.Flags())
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().BoolVar(&deploy, "deploy", false, "Call deploy before run.")
	runCmd.Flags().BoolVar(&force, "force", false, "Force acquisition of deployment lock.")
	runCmd.Flags().BoolVar(&noWait, "no-wait", false, "Don't wait for the run to complete.")
	runCmd.Flags().StringVar(&computeID, "compute", "", "Override compute in the deployment with the given compute ID.")
}
