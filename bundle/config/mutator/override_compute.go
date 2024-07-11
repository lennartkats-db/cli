package mutator

import (
	"context"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/env"
)

type overrideCompute struct{}

func OverrideCompute() bundle.Mutator {
	return &overrideCompute{}
}

func (m *overrideCompute) Name() string {
	return "OverrideCompute"
}

func overrideJobCompute(j *resources.Job, compute string) {
	for i := range j.Tasks {
		var task = &j.Tasks[i]

		if task.ForEachTask != nil {
			task = &task.ForEachTask.Task
		}

		if task.NewCluster != nil || task.ExistingClusterId != "" || task.EnvironmentKey != "" || task.JobClusterKey != "" {
			task.NewCluster = nil
			task.JobClusterKey = ""
			task.EnvironmentKey = ""
			task.ExistingClusterId = compute
		}
	}
}

func (m *overrideCompute) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	if b.Config.Bundle.Mode != config.Development {
		if b.Config.Bundle.ComputeID != "" {
			return diag.Errorf(diag.TargetModeError)("can only override compute for a target that uses 'mode: development'")
		}
		return nil
	}
	if v := env.Get(ctx, "DATABRICKS_CLUSTER_ID"); v != "" {
		b.Config.Bundle.ComputeID = v
	}

	if b.Config.Bundle.ComputeID == "" {
		return nil
	}

	r := b.Config.Resources
	for i := range r.Jobs {
		overrideJobCompute(r.Jobs[i], b.Config.Bundle.ComputeID)
	}

	return nil
}
