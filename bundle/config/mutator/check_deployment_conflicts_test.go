package mutator

import (
	"context"
	"testing"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/stretchr/testify/assert"
)

func TestNoConflict(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Workspace: config.Workspace{RootPath: "/a/b/c"},
		},
	}

	err := DefineDefaultWorkspacePaths().Apply(context.Background(), b)
	assert.NoError(t, err)

	// Existing job from same bundle
	existingJob := jobs.BaseJob{
		Settings: &jobs.JobSettings{
			Deployment: &jobs.JobDeployment{
				// Please don't change the path used in this test;
				// existing v0.2xx CLIs create jobs with this path!
				MetadataFilePath: "/a/b/c/state/metadata.json",
			},
		},
	}
	err = checkJobConflict(b, existingJob)
	assert.NoError(t, err)

	// Existing job without deployment metadata
	// (this might be a job deployed with an old version of the CLI
	//  that didn't create set the metadata property)
	existingJobWithoutMetadata := jobs.BaseJob{
		Settings: &jobs.JobSettings{},
	}
	err = checkJobConflict(b, existingJobWithoutMetadata)
	assert.NoError(t, err)

	// Existing job without deployment metadata path
	// (this might be what deployed jobs look like in the future,
	//  we should just just ignore them and fail in the Terraform
	//  stage if we have to)
	existingJobWithoutMetadataPath := jobs.BaseJob{
		Settings: &jobs.JobSettings{
			Deployment: &jobs.JobDeployment{},
		},
	}
	err = checkJobConflict(b, existingJobWithoutMetadataPath)
	assert.NoError(t, err)
}

func TestConflict(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Workspace: config.Workspace{RootPath: "/a/b/c"},
		},
	}

	err := DefineDefaultWorkspacePaths().Apply(context.Background(), b)
	assert.NoError(t, err)

	// Existing job from a different bundle
	existingJob := jobs.BaseJob{
		Settings: &jobs.JobSettings{
			Deployment: &jobs.JobDeployment{
				MetadataFilePath: "/q/r/p/state/metadata.json",
			},
		},
	}
	err = checkJobConflict(b, existingJob)
	assert.ErrorContains(t, err, "conflict")
}
