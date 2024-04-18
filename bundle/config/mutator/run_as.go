package mutator

import (
	"context"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/databricks-sdk-go/service/jobs"
)

type setRunAs struct {
}

// This mutator does two things:
//
//  1. Sets the run_as field for jobs to the value of the run_as field in the bundle.
//
//  2. Validates that the bundle run_as configuration is valid in the context of the bundle.
//     If the run_as user is different from the current deployment user, DABs only
//     supports a subset of resources.
func SetRunAs() bundle.Mutator {
	return &setRunAs{}
}

func (m *setRunAs) Name() string {
	return "SetRunAs"
}

func validateRunAs(b *bundle.Bundle) diag.Diagnostics {
	runAs := b.Config.RunAs

	// Error if neither service_principal_name nor user_name are specified
	if runAs.ServicePrincipalName == "" && runAs.UserName == "" {
		return diag.Errorf("run_as section must specify exactly one identity. Neither service_principal_name nor user_name is specified at %s", b.Config.GetLocation("run_as"))
	}

	// Error if both service_principal_name and user_name are specified
	if runAs.UserName != "" && runAs.ServicePrincipalName != "" {
		return diag.Diagnostics{{
			Summary:  "run_as section cannot specify both user_name and service_principal_name",
			Location: b.Config.GetLocation("run_as"),
			Severity: diag.Error,
		}}
	}

	identity := runAs.ServicePrincipalName
	if identity == "" {
		identity = runAs.UserName
	}

	// All resources are supported if the run_as identity is the same as the current deployment identity.
	if identity == b.Config.Workspace.CurrentUser.UserName {
		return nil
	}

	// DLT pipelines do not support run_as in the API, so they require owner==run_as
	for _, p := range b.Config.Resources.Pipelines {
		checkValidOwnerForUnsupportedType(b, runAs, "pipelines", p.ID, p.Permissions)
	}

	// Model serving endpoints do not support run_as in the API, so they require owner==run_as
	for _, p := range b.Config.Resources.ModelServingEndpoints {
		checkValidOwnerForUnsupportedType(b, runAs, "model_serving_endpoints", p.ID, p.Permissions)
	}

	return nil
}

func checkValidOwnerForUnsupportedType(b *bundle.Bundle, runas *jobs.JobRunAs, resourceType string, resourceId string, permissions []resources.Permission) diag.Diagnostics {
	for _, p := range permissions {
		if p.Level == "IS_OWNER" {
			if p.UserName == runas.UserName {
				return nil
			}
			if p.ServicePrincipalName == runas.ServicePrincipalName {
				return nil
			}
			return diag.Diagnostics{{
				Summary: fmt.Sprint(
					"%s do not support a setting a run_as user that is different from the owner.\n"+
						"See https://docs.databricks.com/en/dev-tools/bundles/run-as.html to learn more about the run_as property.",
					resourceType,
				),
				Location: b.Config.GetLocation(fmt.Sprint("resources.%s.%s", resourceType, resourceId)),
				Severity: diag.Error,
				ID:       diag.RunAsDenied,
			}}
		}
	}
	return nil
}

func (m *setRunAs) Apply(_ context.Context, b *bundle.Bundle) diag.Diagnostics {
	// Mutator is a no-op if run_as is not specified in the bundle
	runAs := b.Config.RunAs
	if runAs == nil {
		return nil
	}

	// Assert the run_as configuration is valid in the context of the bundle
	if diag := validateRunAs(b); diag != nil {
		return diag
	}

	// Set run_as for jobs
	for i := range b.Config.Resources.Jobs {
		job := b.Config.Resources.Jobs[i]
		if job.RunAs != nil {
			continue
		}
		job.RunAs = &jobs.JobRunAs{
			ServicePrincipalName: runAs.ServicePrincipalName,
			UserName:             runAs.UserName,
		}
	}

	return nil
}
