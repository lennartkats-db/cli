package artifacts

import (
	"context"
	"fmt"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

func UploadAll() bundle.Mutator {
	return &all{
		name: "Upload",
		fn:   uploadArtifactByName,
	}
}

func CleanUp() bundle.Mutator {
	return &cleanUp{}
}

type upload struct {
	name string
}

func uploadArtifactByName(name string) (bundle.Mutator, error) {
	return &upload{name}, nil
}

func (m *upload) Name() string {
	return fmt.Sprintf("artifacts.Upload(%s)", m.name)
}

func (m *upload) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	artifact, ok := b.Config.Artifacts[m.name]
	if !ok {
		return diag.Errorf(diag.ArtifactError)("artifact doesn't exist: %s", m.name)
	}

	if len(artifact.Files) == 0 {
		return diag.Errorf(diag.ArtifactError)("artifact source is not configured: %s", m.name)
	}

	return bundle.Apply(ctx, b, getUploadMutator(artifact.Type, m.name))
}

type cleanUp struct{}

func (m *cleanUp) Name() string {
	return "artifacts.CleanUp"
}

func (m *cleanUp) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	uploadPath, err := getUploadBasePath(b)
	if err != nil {
		return diag.FromErr(diag.ArtifactError, err)
	}

	b.WorkspaceClient().Workspace.Delete(ctx, workspace.Delete{
		Path:      uploadPath,
		Recursive: true,
	})

	err = b.WorkspaceClient().Workspace.MkdirsByPath(ctx, uploadPath)
	if err != nil {
		return diag.Errorf(diag.IOError)("unable to create directory for %s: %v", uploadPath, err)
	}

	return nil
}
