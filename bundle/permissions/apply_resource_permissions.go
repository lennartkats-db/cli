package permissions

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/diag"
)

const CAN_MANAGE = "CAN_MANAGE"
const CAN_VIEW = "CAN_VIEW"
const CAN_RUN = "CAN_RUN"

// The owner permission, which cannot be set directly by the user.
// This permission may be assigned to one of the "CAN_MANAGE" users,
// depending on the value of run_as.
const IS_OWNER = "IS_OWNER"

var allowedLevels = []string{CAN_MANAGE, CAN_VIEW, CAN_RUN}
var levelsMap = map[string](map[string]string){
	"jobs": {
		IS_OWNER:   "IS_OWNER",
		CAN_MANAGE: "CAN_MANAGE",
		CAN_VIEW:   "CAN_VIEW",
		CAN_RUN:    "CAN_MANAGE_RUN",
	},
	"pipelines": {
		IS_OWNER:   "IS_OWNER",
		CAN_MANAGE: "CAN_MANAGE",
		CAN_VIEW:   "CAN_VIEW",
		CAN_RUN:    "CAN_RUN",
	},
	"mlflow_experiments": {
		IS_OWNER:   "IS_OWNER",
		CAN_MANAGE: "CAN_MANAGE",
		CAN_VIEW:   "CAN_READ",
	},
	"mlflow_models": {
		IS_OWNER:   "IS_OWNER",
		CAN_MANAGE: "CAN_MANAGE",
		CAN_VIEW:   "CAN_READ",
	},
	"model_serving_endpoints": {
		IS_OWNER:   "IS_OWNER",
		CAN_MANAGE: "CAN_MANAGE",
		CAN_VIEW:   "CAN_VIEW",
		CAN_RUN:    "CAN_QUERY",
	},
}

type applyResourcePermissions struct{}

func ApplyResourcePermissions() bundle.Mutator {
	return &applyResourcePermissions{}
}

func (m *applyResourcePermissions) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	err := validate(b)
	if err != nil {
		return diag.FromErr(err)
	}

	if b.Config.RunAs != nil && b.Config.RunAs.UserName != "" {
		// If run_as is set to a human user, we make that user the owner of all resources.
		// This makes it possible for other "can manage" users to redeploy a resource
		// without getting an error that the owner is being changed. Changing
		// the owner is generally not allowed (only admins can do this for some
		// resources but we don't want to special case for admins).
		b.Config.Permissions = append(b.Config.Permissions, resources.Permission{
			Level:    IS_OWNER,
			UserName: b.Config.RunAs.UserName,
		})
	}

	applyForJobs(ctx, b)
	applyForPipelines(ctx, b)
	applyForMlModels(ctx, b)
	applyForMlExperiments(ctx, b)
	applyForModelServiceEndpoints(ctx, b)

	return nil
}

func validate(b *bundle.Bundle) error {
	for _, p := range b.Config.Permissions {
		if !slices.Contains(allowedLevels, p.Level) {
			return fmt.Errorf("invalid permission level: %s, allowed values: [%s]", p.Level, strings.Join(allowedLevels, ", "))
		}
	}

	return nil
}

func applyForJobs(ctx context.Context, b *bundle.Bundle) {
	for key, job := range b.Config.Resources.Jobs {
		job.Permissions = append(job.Permissions, convert(
			ctx,
			b.Config.Permissions,
			job.Permissions,
			key,
			levelsMap["jobs"],
		)...)
	}
}

func applyForPipelines(ctx context.Context, b *bundle.Bundle) {
	for key, pipeline := range b.Config.Resources.Pipelines {
		pipeline.Permissions = append(pipeline.Permissions, convert(
			ctx,
			b.Config.Permissions,
			pipeline.Permissions,
			key,
			levelsMap["pipelines"],
		)...)
	}
}

func applyForMlExperiments(ctx context.Context, b *bundle.Bundle) {
	for key, experiment := range b.Config.Resources.Experiments {
		experiment.Permissions = append(experiment.Permissions, convert(
			ctx,
			b.Config.Permissions,
			experiment.Permissions,
			key,
			levelsMap["mlflow_experiments"],
		)...)
	}
}

func applyForMlModels(ctx context.Context, b *bundle.Bundle) {
	for key, model := range b.Config.Resources.Models {
		model.Permissions = append(model.Permissions, convert(
			ctx,
			b.Config.Permissions,
			model.Permissions,
			key,
			levelsMap["mlflow_models"],
		)...)
	}
}

func applyForModelServiceEndpoints(ctx context.Context, b *bundle.Bundle) {
	for key, model := range b.Config.Resources.ModelServingEndpoints {
		model.Permissions = append(model.Permissions, convert(
			ctx,
			b.Config.Permissions,
			model.Permissions,
			key,
			levelsMap["model_serving_endpoints"],
		)...)
	}
}

func (m *applyResourcePermissions) Name() string {
	return "ApplyResourcePermissions"
}
