package metadata

import (
	"context"
	"path"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/databricks-sdk-go/service/jobs"
)

type annotateJobs struct{}

func AnnotateJobs() bundle.Mutator {
	return &annotateJobs{}
}

func (m *annotateJobs) Name() string {
	return "metadata.AnnotateJobs"
}

func (m *annotateJobs) Apply(_ context.Context, b *bundle.Bundle) error {
	for _, job := range b.Config.Resources.Jobs {
		if job.JobSettings == nil {
			continue
		}

		job.JobSettings.Deployment = &jobs.JobDeployment{
			Kind:             jobs.JobDeploymentKindBundle,
			MetadataFilePath: CreateMetadataFilePath(b),
		}
		job.JobSettings.EditMode = jobs.JobSettingsEditModeUiLocked
		job.JobSettings.Format = jobs.FormatMultiTask
	}

	return nil
}

func CreateMetadataFilePath(b *bundle.Bundle) string {
	return path.Join(b.Config.Workspace.StatePath, MetadataFileName)
}
