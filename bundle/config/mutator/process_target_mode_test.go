package mutator

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/tags"
	sdkconfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/databricks/databricks-sdk-go/service/ml"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/databricks/databricks-sdk-go/service/serving"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockBundle(mode config.Mode) *bundle.Bundle {
	return &bundle.Bundle{
		Config: config.Root{
			Bundle: config.Bundle{
				Mode: mode,
				Git: config.Git{
					OriginURL: "http://origin",
					Branch:    "main",
				},
			},
			Targets: map[string]*config.Target{
				"": {},
			},
			Workspace: config.Workspace{
				CurrentUser: &config.User{
					ShortName: "lennart",
					User: &iam.User{
						UserName: "lennart@company.com",
						Id:       "1",
					},
				},
				StatePath:    "/Users/lennart@company.com/.bundle/x/y/state",
				ArtifactPath: "/Users/lennart@company.com/.bundle/x/y/artifacts",
				FilePath:     "/Users/lennart@company.com/.bundle/x/y/files",
			},
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"job1": {
						JobSettings: &jobs.JobSettings{
							Name: "job1",
							Schedule: &jobs.CronSchedule{
								QuartzCronExpression: "* * * * *",
							},
						},
					},
					"job2": {
						JobSettings: &jobs.JobSettings{
							Name: "job2",
							Schedule: &jobs.CronSchedule{
								QuartzCronExpression: "* * * * *",
								PauseStatus:          jobs.PauseStatusUnpaused,
							},
						},
					},
					"job3": {
						JobSettings: &jobs.JobSettings{
							Name: "job3",
							Trigger: &jobs.TriggerSettings{
								FileArrival: &jobs.FileArrivalTriggerConfiguration{
									Url: "test.com",
								},
							},
						},
					},
					"job4": {
						JobSettings: &jobs.JobSettings{
							Name: "job4",
							Continuous: &jobs.Continuous{
								PauseStatus: jobs.PauseStatusPaused,
							},
						},
					},
				},
				Pipelines: map[string]*resources.Pipeline{
					"pipeline1": {PipelineSpec: &pipelines.PipelineSpec{Name: "pipeline1"}},
				},
				Experiments: map[string]*resources.MlflowExperiment{
					"experiment1": {Experiment: &ml.Experiment{Name: "/Users/lennart.kats@databricks.com/experiment1"}},
					"experiment2": {Experiment: &ml.Experiment{Name: "experiment2"}},
				},
				Models: map[string]*resources.MlflowModel{
					"model1": {Model: &ml.Model{Name: "model1"}},
				},
				ModelServingEndpoints: map[string]*resources.ModelServingEndpoint{
					"servingendpoint1": {CreateServingEndpoint: &serving.CreateServingEndpoint{Name: "servingendpoint1"}},
				},
				RegisteredModels: map[string]*resources.RegisteredModel{
					"registeredmodel1": {CreateRegisteredModelRequest: &catalog.CreateRegisteredModelRequest{Name: "registeredmodel1"}},
				},
			},
		},
		// Use AWS implementation for testing.
		Tagging: tags.ForCloud(&sdkconfig.Config{
			Host: "https://company.cloud.databricks.com",
		}),
	}
}

func TestProcessTargetModeDevelopment(t *testing.T) {
	b := mockBundle(config.Development)

	m := ProcessTargetMode()
	diags := bundle.Apply(context.Background(), b, m)
	require.NoError(t, diags.Error())

	// Job 1
	assert.Equal(t, "[dev lennart] job1", b.Config.Resources.Jobs["job1"].Name)
	assert.Equal(t, b.Config.Resources.Jobs["job1"].Tags["dev"], "lennart")
	assert.Equal(t, b.Config.Resources.Jobs["job1"].Schedule.PauseStatus, jobs.PauseStatusPaused)

	// Job 2
	assert.Equal(t, "[dev lennart] job2", b.Config.Resources.Jobs["job2"].Name)
	assert.Equal(t, b.Config.Resources.Jobs["job2"].Tags["dev"], "lennart")
	assert.Equal(t, b.Config.Resources.Jobs["job2"].Schedule.PauseStatus, jobs.PauseStatusUnpaused)

	// Pipeline 1
	assert.Equal(t, "[dev lennart] pipeline1", b.Config.Resources.Pipelines["pipeline1"].Name)
	assert.True(t, b.Config.Resources.Pipelines["pipeline1"].PipelineSpec.Development)

	// Experiment 1
	assert.Equal(t, "/Users/lennart.kats@databricks.com/[dev lennart] experiment1", b.Config.Resources.Experiments["experiment1"].Name)
	assert.Contains(t, b.Config.Resources.Experiments["experiment1"].Experiment.Tags, ml.ExperimentTag{Key: "dev", Value: "lennart"})
	assert.Equal(t, "dev", b.Config.Resources.Experiments["experiment1"].Experiment.Tags[0].Key)

	// Experiment 2
	assert.Equal(t, "[dev lennart] experiment2", b.Config.Resources.Experiments["experiment2"].Name)
	assert.Contains(t, b.Config.Resources.Experiments["experiment2"].Experiment.Tags, ml.ExperimentTag{Key: "dev", Value: "lennart"})

	// Model 1
	assert.Equal(t, "[dev lennart] model1", b.Config.Resources.Models["model1"].Name)
	assert.Contains(t, b.Config.Resources.Models["model1"].Tags, ml.ModelTag{Key: "dev", Value: "lennart"})

	// Model serving endpoint 1
	assert.Equal(t, "dev_lennart_servingendpoint1", b.Config.Resources.ModelServingEndpoints["servingendpoint1"].Name)

	// Registered model 1
	assert.Equal(t, "dev_lennart_registeredmodel1", b.Config.Resources.RegisteredModels["registeredmodel1"].Name)
}

func TestProcessTargetModeDevelopmentTagNormalizationForAws(t *testing.T) {
	b := mockBundle(config.Development)
	b.Tagging = tags.ForCloud(&sdkconfig.Config{
		Host: "https://dbc-XXXXXXXX-YYYY.cloud.databricks.com/",
	})

	b.Config.Workspace.CurrentUser.ShortName = "Héllö wörld?!"
	diags := bundle.Apply(context.Background(), b, ProcessTargetMode())
	require.NoError(t, diags.Error())

	// Assert that tag normalization took place.
	assert.Equal(t, "Hello world__", b.Config.Resources.Jobs["job1"].Tags["dev"])
}

func TestProcessTargetModeDevelopmentTagNormalizationForAzure(t *testing.T) {
	b := mockBundle(config.Development)
	b.Tagging = tags.ForCloud(&sdkconfig.Config{
		Host: "https://adb-xxx.y.azuredatabricks.net/",
	})

	b.Config.Workspace.CurrentUser.ShortName = "Héllö wörld?!"
	diags := bundle.Apply(context.Background(), b, ProcessTargetMode())
	require.NoError(t, diags.Error())

	// Assert that tag normalization took place (Azure allows more characters than AWS).
	assert.Equal(t, "Héllö wörld?!", b.Config.Resources.Jobs["job1"].Tags["dev"])
}

func TestProcessTargetModeDevelopmentTagNormalizationForGcp(t *testing.T) {
	b := mockBundle(config.Development)
	b.Tagging = tags.ForCloud(&sdkconfig.Config{
		Host: "https://123.4.gcp.databricks.com/",
	})

	b.Config.Workspace.CurrentUser.ShortName = "Héllö wörld?!"
	diags := bundle.Apply(context.Background(), b, ProcessTargetMode())
	require.NoError(t, diags.Error())

	// Assert that tag normalization took place.
	assert.Equal(t, "Hello_world", b.Config.Resources.Jobs["job1"].Tags["dev"])
}

func TestProcessTargetModeDefault(t *testing.T) {
	b := mockBundle("")

	m := ProcessTargetMode()
	diags := bundle.Apply(context.Background(), b, m)
	require.NoError(t, diags.Error())
	assert.Equal(t, "job1", b.Config.Resources.Jobs["job1"].Name)
	assert.Equal(t, "pipeline1", b.Config.Resources.Pipelines["pipeline1"].Name)
	assert.False(t, b.Config.Resources.Pipelines["pipeline1"].PipelineSpec.Development)
	assert.Equal(t, "servingendpoint1", b.Config.Resources.ModelServingEndpoints["servingendpoint1"].Name)
	assert.Equal(t, "registeredmodel1", b.Config.Resources.RegisteredModels["registeredmodel1"].Name)
}

func TestProcessTargetModeProduction(t *testing.T) {
	b := mockBundle(config.Production)

	diags := validateProductionMode(context.Background(), b, false)
	require.ErrorContains(t, diags.Error(), "production")

	b.Config.Workspace.StatePath = "/Shared/.bundle/x/y/state"
	b.Config.Workspace.ArtifactPath = "/Shared/.bundle/x/y/artifacts"
	b.Config.Workspace.FilePath = "/Shared/.bundle/x/y/files"

	diags = validateProductionMode(context.Background(), b, false)
	require.ErrorContains(t, diags.Error(), "production")

	permissions := []resources.Permission{
		{
			Level:    "CAN_MANAGE",
			UserName: "user@company.com",
		},
	}
	b.Config.Resources.Jobs["job1"].Permissions = permissions
	b.Config.Resources.Jobs["job1"].RunAs = &jobs.JobRunAs{UserName: "user@company.com"}
	b.Config.Resources.Jobs["job2"].RunAs = &jobs.JobRunAs{UserName: "user@company.com"}
	b.Config.Resources.Jobs["job3"].RunAs = &jobs.JobRunAs{UserName: "user@company.com"}
	b.Config.Resources.Jobs["job4"].RunAs = &jobs.JobRunAs{UserName: "user@company.com"}
	b.Config.Resources.Pipelines["pipeline1"].Permissions = permissions
	b.Config.Resources.Experiments["experiment1"].Permissions = permissions
	b.Config.Resources.Experiments["experiment2"].Permissions = permissions
	b.Config.Resources.Models["model1"].Permissions = permissions
	b.Config.Resources.ModelServingEndpoints["servingendpoint1"].Permissions = permissions

	diags = validateProductionMode(context.Background(), b, false)
	require.NoError(t, diags.Error())

	assert.Equal(t, "job1", b.Config.Resources.Jobs["job1"].Name)
	assert.Equal(t, "pipeline1", b.Config.Resources.Pipelines["pipeline1"].Name)
	assert.False(t, b.Config.Resources.Pipelines["pipeline1"].PipelineSpec.Development)
	assert.Equal(t, "servingendpoint1", b.Config.Resources.ModelServingEndpoints["servingendpoint1"].Name)
	assert.Equal(t, "registeredmodel1", b.Config.Resources.RegisteredModels["registeredmodel1"].Name)
}

func TestProcessTargetModeProductionOkForPrincipal(t *testing.T) {
	b := mockBundle(config.Production)

	// Our target has all kinds of problems when not using service principals ...
	diags := validateProductionMode(context.Background(), b, false)
	require.Error(t, diags.Error())

	// ... but we're much less strict when a principal is used
	diags = validateProductionMode(context.Background(), b, true)
	require.NoError(t, diags.Error())
}

func TestProcessTargetModeProductionOkWithRootPath(t *testing.T) {
	b := mockBundle(config.Production)

	// Our target has all kinds of problems when not using service principals ...
	diags := validateProductionMode(context.Background(), b, false)
	require.Error(t, diags.Error())

	// ... but we're okay if we specify a root path
	b.Config.Targets[""].Workspace = &config.Workspace{
		RootPath: "some-root-path",
	}
	diags = validateProductionMode(context.Background(), b, false)
	require.NoError(t, diags.Error())
}

// Make sure that we have test coverage for all resource types
func TestAllResourcesMocked(t *testing.T) {
	b := mockBundle(config.Development)
	resources := reflect.ValueOf(b.Config.Resources)

	for i := 0; i < resources.NumField(); i++ {
		field := resources.Field(i)
		if field.Kind() == reflect.Map {
			assert.True(
				t,
				!field.IsNil() && field.Len() > 0,
				"process_target_mode should support '%s' (please add it to process_target_mode.go and extend the test suite)",
				resources.Type().Field(i).Name,
			)
		}
	}
}

// Make sure that we at least rename all resources
func TestAllResourcesRenamed(t *testing.T) {
	b := mockBundle(config.Development)

	m := ProcessTargetMode()
	diags := bundle.Apply(context.Background(), b, m)
	require.NoError(t, diags.Error())

	resources := reflect.ValueOf(b.Config.Resources)
	for i := 0; i < resources.NumField(); i++ {
		field := resources.Field(i)

		if field.Kind() == reflect.Map {
			for _, key := range field.MapKeys() {
				resource := field.MapIndex(key)
				nameField := resource.Elem().FieldByName("Name")
				if nameField.IsValid() && nameField.Kind() == reflect.String {
					assert.True(
						t,
						strings.Contains(nameField.String(), "dev"),
						"process_target_mode should rename '%s' in '%s'",
						key,
						resources.Type().Field(i).Name,
					)
				}
			}
		}
	}
}
