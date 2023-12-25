package mutator

import (
	"context"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/bundle/deploy/metadata"
	"github.com/databricks/databricks-sdk-go/service/jobs"
)

type checkDeploymentConflicts struct{}

// Make sure that resources we deploy don't already exist in the target workspace.
func CheckDeploymentConflicts() bundle.Mutator {
	return &checkDeploymentConflicts{}
}

func (m *checkDeploymentConflicts) Name() string {
	return "checkDeploymentConflicts"
}

func (m *checkDeploymentConflicts) Apply(ctx context.Context, b *bundle.Bundle) error {
	if b.Config.Bundle.Mode != config.Production {
		return nil
	}
	// We only check against jobs in this mutator since other resources don't have
	// metadata indicating their source bundle. What helps for other resources
	// is that they return an error when a duplicate name is being used.
	// We just can't show the same helpful error for them.
	if len(b.Config.Resources.Jobs) == 0 {
		return nil
	}

	// Take one of the jobs to be deployed
	var job *resources.Job
	for _, job = range b.Config.Resources.Jobs {
		break
	}

	// Find a job with the same name in the workspace
	w := b.WorkspaceClient()
	list := w.Jobs.List(ctx, jobs.ListJobsRequest{
		Name: job.Name,
	})
	existingJob, err := list.Next(ctx)
	if err != nil {
		return nil
	}

	return checkJobConflict(b, existingJob)
}

func checkJobConflict(b *bundle.Bundle, existingJob jobs.BaseJob) error {
	metadataPath := metadata.CreateMetadataFilePath(b)

	if existingJob.Settings.Deployment == nil || existingJob.Settings.Deployment.MetadataFilePath == "" {
		return nil
	}

	existingPath := existingJob.Settings.Deployment.MetadataFilePath
	if existingPath != metadataPath {
		return fmt.Errorf(`conflicting deployment: job with name '%s' already exists in the target workspace but was deployed from a different bundle or user (existing state path: '%s')`, existingJob.Settings.Name, existingPath)
	}
	return nil
}
