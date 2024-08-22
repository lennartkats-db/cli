package permissions

import (
	"context"
	"testing"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/stretchr/testify/require"
)

func TestUpdateResourcePermissions(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Workspace: config.Workspace{
				RootPath: "/Users/foo@bar.com",
			},
			Permissions: []resources.Permission{
				{Level: CAN_MANAGE, UserName: "TestUser"},
				{Level: CAN_VIEW, GroupName: "TestGroup"},
				{Level: CAN_RUN, ServicePrincipalName: "TestServicePrincipal"},
			},
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"job_1": {
						JobSettings: &jobs.JobSettings{
							Name: "job_1",
						},
					},
					"job_2": {
						JobSettings: &jobs.JobSettings{
							Name: "job_2",
						},
					},
				},
				Pipelines: map[string]*resources.Pipeline{
					"pipeline_1": {},
					"pipeline_2": {},
				},
				Models: map[string]*resources.MlflowModel{
					"model_1": {},
					"model_2": {},
				},
				Experiments: map[string]*resources.MlflowExperiment{
					"experiment_1": {},
					"experiment_2": {},
				},
				ModelServingEndpoints: map[string]*resources.ModelServingEndpoint{
					"endpoint_1": {},
					"endpoint_2": {},
				},
			},
		},
	}

	diags := bundle.Apply(context.Background(), b, UpdateResourcePermissions())
	require.NoError(t, diags.Error())

	require.Len(t, b.Config.Resources.Jobs["job_1"].Permissions, 3)
	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "CAN_MANAGE_RUN", ServicePrincipalName: "TestServicePrincipal"})

	require.Len(t, b.Config.Resources.Jobs["job_2"].Permissions, 3)
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "CAN_MANAGE_RUN", ServicePrincipalName: "TestServicePrincipal"})

	require.Len(t, b.Config.Resources.Pipelines["pipeline_1"].Permissions, 3)
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_1"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_1"].Permissions, resources.Permission{Level: "CAN_RUN", ServicePrincipalName: "TestServicePrincipal"})

	require.Len(t, b.Config.Resources.Pipelines["pipeline_2"].Permissions, 3)
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_2"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_2"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_2"].Permissions, resources.Permission{Level: "CAN_RUN", ServicePrincipalName: "TestServicePrincipal"})

	require.Len(t, b.Config.Resources.Models["model_1"].Permissions, 2)
	require.Contains(t, b.Config.Resources.Models["model_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Models["model_1"].Permissions, resources.Permission{Level: "CAN_READ", GroupName: "TestGroup"})

	require.Len(t, b.Config.Resources.Models["model_2"].Permissions, 2)
	require.Contains(t, b.Config.Resources.Models["model_2"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Models["model_2"].Permissions, resources.Permission{Level: "CAN_READ", GroupName: "TestGroup"})

	require.Len(t, b.Config.Resources.Experiments["experiment_1"].Permissions, 2)
	require.Contains(t, b.Config.Resources.Experiments["experiment_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Experiments["experiment_1"].Permissions, resources.Permission{Level: "CAN_READ", GroupName: "TestGroup"})

	require.Len(t, b.Config.Resources.Experiments["experiment_2"].Permissions, 2)
	require.Contains(t, b.Config.Resources.Experiments["experiment_2"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Experiments["experiment_2"].Permissions, resources.Permission{Level: "CAN_READ", GroupName: "TestGroup"})

	require.Len(t, b.Config.Resources.ModelServingEndpoints["endpoint_1"].Permissions, 3)
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_1"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_1"].Permissions, resources.Permission{Level: "CAN_QUERY", ServicePrincipalName: "TestServicePrincipal"})

	require.Len(t, b.Config.Resources.ModelServingEndpoints["endpoint_2"].Permissions, 3)
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_2"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_2"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_2"].Permissions, resources.Permission{Level: "CAN_QUERY", ServicePrincipalName: "TestServicePrincipal"})
}

func TestUpdateResourcePermissionsWarningOnOverlapPermission(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Workspace: config.Workspace{
				RootPath: "/Users/foo@bar.com",
			},
			Permissions: []resources.Permission{
				{Level: CAN_MANAGE, UserName: "TestUser"},
				{Level: CAN_VIEW, GroupName: "TestGroup"},
			},
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"job_1": {
						JobSettings: &jobs.JobSettings{
							Name: "job_1",
						},
						Permissions: []resources.Permission{
							{Level: CAN_VIEW, UserName: "TestUser"},
						},
					},
					"job_2": {
						JobSettings: &jobs.JobSettings{
							Name: "job_2",
						},
						Permissions: []resources.Permission{
							{Level: CAN_VIEW, UserName: "TestUser2"},
						},
					},
				},
			},
		},
	}

	diags := bundle.Apply(context.Background(), b, UpdateResourcePermissions())
	require.NoError(t, diags.Error())

	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "CAN_VIEW", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "CAN_VIEW", UserName: "TestUser2"})
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "TestUser"})
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "CAN_VIEW", GroupName: "TestGroup"})

}

func TestUpdateResourcePermissionsOwnerLevel(t *testing.T) {
	// Create a bundle with a run_as configuration
	b := &bundle.Bundle{
		Config: config.Root{
			Permissions: []resources.Permission{
				{Level: IS_OWNER, UserName: "Alice"},
			},
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"job_1": {
						Permissions: []resources.Permission{
							{Level: CAN_MANAGE, UserName: "Bob"},
						},
						JobSettings: &jobs.JobSettings{
							Name: "job_1",
						},
					},
					"job_2": {
						Permissions: []resources.Permission{
							{Level: IS_OWNER, UserName: "Bob"},
						},
						JobSettings: &jobs.JobSettings{
							Name: "job_1",
						},
					},
				},
				Pipelines: map[string]*resources.Pipeline{
					"pipeline_1": {},
				},
				Models: map[string]*resources.MlflowModel{
					"model_1": {
						Permissions: []resources.Permission{
							{Level: CAN_MANAGE, UserName: "Bob"},
						},
					},
				},
				Experiments: map[string]*resources.MlflowExperiment{
					"experiment_1": {},
				},
				ModelServingEndpoints: map[string]*resources.ModelServingEndpoint{
					"endpoint_1": {},
				},
			},
		},
	}

	diags := bundle.Apply(context.Background(), b, UpdateResourcePermissions())
	require.NoError(t, diags.Error())
	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "Bob"})
	require.Contains(t, b.Config.Resources.Jobs["job_1"].Permissions, resources.Permission{Level: "IS_OWNER", UserName: "Alice"})
	require.Contains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "IS_OWNER", UserName: "Bob"})
	require.NotContains(t, b.Config.Resources.Jobs["job_2"].Permissions, resources.Permission{Level: "IS_OWNER", UserName: "Alice"})
	require.Contains(t, b.Config.Resources.Pipelines["pipeline_1"].Permissions, resources.Permission{Level: "IS_OWNER", UserName: "Alice"})
	require.Contains(t, b.Config.Resources.Experiments["experiment_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "Alice"})
	require.Contains(t, b.Config.Resources.Models["model_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "Alice"})
	require.Contains(t, b.Config.Resources.Models["model_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "Bob"})
	require.Contains(t, b.Config.Resources.ModelServingEndpoints["endpoint_1"].Permissions, resources.Permission{Level: "CAN_MANAGE", UserName: "Alice"})
}
