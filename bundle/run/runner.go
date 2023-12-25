package run

import (
	"context"
	"fmt"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/deploy/terraform"
	"github.com/databricks/cli/bundle/phases"
	"github.com/databricks/cli/bundle/run/output"
	"github.com/databricks/cli/libs/cmdio"
)

type key string

func (k key) Key() string {
	return string(k)
}

// Runner defines the interface for a runnable resource (or workload).
type Runner interface {
	// Key returns the fully qualified (unique) identifier for this runnable resource.
	// This is used for showing the user hints w.r.t. disambiguation.
	Key() string

	// Name returns the resource's name, if defined.
	Name() string

	// Returns the canonical URL for the resource.
	URL() string

	// Determines if the underlying resource is runnable.
	IsRunnable() bool

	// Run the underlying worklow.
	Run(ctx context.Context, opts *Options) (output.RunOutput, error)
}

// find locates a runner matching the specified argument.
//
// Its behavior is as follows:
//  1. Try to find a resource with <key> identical to the argument.
//  2. Try to find a resource with <type>.<key> identical to the argument.
//
// If an argument resolves to multiple resources, it returns an error.
func find(b *bundle.Bundle, arg string) (Runner, error) {
	keyOnly, keyWithType := ResourceKeys(b)
	if len(keyWithType) == 0 {
		return nil, fmt.Errorf("bundle defines no resources")
	}

	runners, ok := keyOnly[arg]
	if !ok {
		runners, ok = keyWithType[arg]
		if !ok {
			return nil, fmt.Errorf("no such resource: %s", arg)
		}
	}

	if len(runners) != 1 {
		var keys []string
		for _, runner := range runners {
			keys = append(keys, runner.Key())
		}
		return nil, fmt.Errorf("ambiguous: %s (can resolve to all of %s)", arg, strings.Join(keys, ", "))
	}

	return runners[0], nil
}

func GetRunnerForCommand(ctx context.Context, args []string, onlyShowRunnables bool) (Runner, error) {
	b := bundle.Get(ctx)

	err := bundle.Apply(ctx, b, bundle.Seq(
		phases.Initialize(),
		terraform.Interpolate(),
		terraform.Write(),
		terraform.StatePull(),
		terraform.Load(terraform.ErrorOnEmptyState),
	))
	if err != nil {
		return nil, err
	}

	// If no arguments are specified, prompt the user to select something to run.
	if len(args) == 0 && cmdio.IsInteractive(ctx) {
		// Invert completions from KEY -> NAME, to NAME -> KEY.
		inv := make(map[string]string)
		for k, v := range ResourceCompletionMap(b, onlyShowRunnables) {
			inv[v] = k
		}
		id, err := cmdio.Select(ctx, inv, "Resource to run")
		if err != nil {
			return nil, err
		}
		args = append(args, id)
	}

	if len(args) != 1 {
		return nil, fmt.Errorf("expected a KEY of the resource to run")
	}

	return find(b, args[0])
}
