package permissions

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/log"
)

const CAN_MANAGE = "CAN_MANAGE"
const CAN_VIEW = "CAN_VIEW"
const CAN_RUN = "CAN_RUN"

// The owner permission, which cannot be set at the bundle level by the user.
// Instead of allowing the user to set this permission we expose
// the run_as property.
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

	defaultOwner := ""
	if b.Config.Experimental != nil && b.Config.Experimental.NewPermissionModel &&
		b.Config.RunAs != nil && b.Config.RunAs.UserName != "" {
		// If IS_OWNER property of individual resources is not set, we default it to the
		// user specified in the run_as property.
		//
		// We prefer using the run_as identity over the deployment identity:
		// - It provides consistency across deploys even if deploys are done by different identities.
		// - It avoids errors when a user different than the run_as identity attempts to deploy.
		//
		// As an example, we for a bundle with run_as=Alice we set owner=Alice. This makes
		// it possible for Bob and Charlie redeploy without getting an error that they can't
		// change the owner to themselves. (To do so they would either explicitly need
		// to change the owner, or they would need to change the run_as!)
		log.Infof(ctx, "Setting default IS_OWNER permissions to user_name %s based on run_as property", b.Config.RunAs.UserName)
		defaultOwner = b.Config.RunAs.UserName
	}

	applyForJobs(ctx, b, defaultOwner)
	applyForPipelines(ctx, b, defaultOwner)
	applyForMlModels(ctx, b, defaultOwner)
	applyForMlExperiments(ctx, b, defaultOwner)
	applyForModelServiceEndpoints(ctx, b, defaultOwner)

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

func applyForJobs(ctx context.Context, b *bundle.Bundle, defaultOwner string) {
	for key, job := range b.Config.Resources.Jobs {
		job.Permissions = extendPermissions(job.Permissions, convert(
			ctx,
			b.Config.Permissions,
			job.Permissions,
			key,
			levelsMap["jobs"],
		), defaultOwner)
	}
}

func applyForPipelines(ctx context.Context, b *bundle.Bundle, defaultOwner string) {
	for key, pipeline := range b.Config.Resources.Pipelines {
		pipeline.Permissions = extendPermissions(pipeline.Permissions, convert(
			ctx,
			b.Config.Permissions,
			pipeline.Permissions,
			key,
			levelsMap["pipelines"],
		), defaultOwner)
	}
}

func applyForMlExperiments(ctx context.Context, b *bundle.Bundle, defaultOwner string) {
	for key, experiment := range b.Config.Resources.Experiments {
		experiment.Permissions = extendPermissions(experiment.Permissions, convert(
			ctx,
			b.Config.Permissions,
			experiment.Permissions,
			key,
			levelsMap["mlflow_experiments"],
		), defaultOwner)
	}
}

func applyForMlModels(ctx context.Context, b *bundle.Bundle, defaultOwner string) {
	for key, model := range b.Config.Resources.Models {
		model.Permissions = extendPermissions(model.Permissions, convert(
			ctx,
			b.Config.Permissions,
			model.Permissions,
			key,
			levelsMap["mlflow_models"],
		), defaultOwner)
	}
}

func applyForModelServiceEndpoints(ctx context.Context, b *bundle.Bundle, defaultOwner string) {
	for key, model := range b.Config.Resources.ModelServingEndpoints {
		model.Permissions = extendPermissions(model.Permissions, convert(
			ctx,
			b.Config.Permissions,
			model.Permissions,
			key,
			levelsMap["model_serving_endpoints"],
		), defaultOwner)
	}
}

func extendPermissions(permissions []resources.Permission, newPermissions []resources.Permission, defaultOwner string) []resources.Permission {
	if defaultOwner != "" {
		alreadyHasOwner := false
		for _, p := range permissions {
			if p.Level == IS_OWNER {
				alreadyHasOwner = true
			}
		}
		if !alreadyHasOwner {
			newPermissions = append(newPermissions, resources.Permission{Level: IS_OWNER, UserName: defaultOwner})
		}
	}

	return append(permissions, newPermissions...)
}

func (m *applyResourcePermissions) Name() string {
	return "ApplyResourcePermissions"
}
