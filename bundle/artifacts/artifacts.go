package artifacts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/artifacts/whl"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/cmdio"
	"github.com/databricks/cli/libs/diag"
	"github.com/databricks/cli/libs/filer"
	"github.com/databricks/cli/libs/log"
)

type mutatorFactory = func(name string) bundle.Mutator

var buildMutators map[config.ArtifactType]mutatorFactory = map[config.ArtifactType]mutatorFactory{
	config.ArtifactPythonWheel: whl.Build,
}

var uploadMutators map[config.ArtifactType]mutatorFactory = map[config.ArtifactType]mutatorFactory{}

func getBuildMutator(t config.ArtifactType, name string) bundle.Mutator {
	mutatorFactory, ok := buildMutators[t]
	if !ok {
		mutatorFactory = BasicBuild
	}

	return mutatorFactory(name)
}

func getUploadMutator(t config.ArtifactType, name string) bundle.Mutator {
	mutatorFactory, ok := uploadMutators[t]
	if !ok {
		mutatorFactory = BasicUpload
	}

	return mutatorFactory(name)
}

// Basic Build defines a general build mutator which builds artifact based on artifact.BuildCommand
type basicBuild struct {
	name string
}

func BasicBuild(name string) bundle.Mutator {
	return &basicBuild{name: name}
}

func (m *basicBuild) Name() string {
	return fmt.Sprintf("artifacts.Build(%s)", m.name)
}

func (m *basicBuild) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	artifact, ok := b.Config.Artifacts[m.name]
	if !ok {
		return diag.Errorf(diag.ArtifactError)("artifact doesn't exist: %s", m.name)
	}

	cmdio.LogString(ctx, fmt.Sprintf("Building %s...", m.name))

	out, err := artifact.Build(ctx)
	if err != nil {
		return diag.Errorf(diag.ArtifactError)("build for %s failed, error: %v, output: %s", m.name, err, out)
	}
	log.Infof(ctx, "Build succeeded")

	return nil
}

// Basic Upload defines a general upload mutator which uploads artifact as a library to workspace
type basicUpload struct {
	name string
}

func BasicUpload(name string) bundle.Mutator {
	return &basicUpload{name: name}
}

func (m *basicUpload) Name() string {
	return fmt.Sprintf("artifacts.Upload(%s)", m.name)
}

func (m *basicUpload) Apply(ctx context.Context, b *bundle.Bundle) diag.Diagnostics {
	artifact, ok := b.Config.Artifacts[m.name]
	if !ok {
		return diag.Errorf(diag.ArtifactError)("artifact doesn't exist: %s", m.name)
	}

	if len(artifact.Files) == 0 {
		return diag.Errorf(diag.ArtifactError)("artifact source is not configured: %s", m.name)
	}

	uploadPath, err := getUploadBasePath(b)
	if err != nil {
		return diag.FromErr(diag.ArtifactError, err)
	}

	client, err := filer.NewWorkspaceFilesClient(b.WorkspaceClient(), uploadPath)
	if err != nil {
		return diag.FromErr(diag.ArtifactError, err)
	}

	err = uploadArtifact(ctx, b, artifact, uploadPath, client)
	if err != nil {
		return diag.Errorf(diag.ArtifactError)("upload for %s failed, error: %v", m.name, err)
	}

	return nil
}

func uploadArtifact(ctx context.Context, b *bundle.Bundle, a *config.Artifact, uploadPath string, client filer.Filer) error {
	for i := range a.Files {
		f := &a.Files[i]

		filename := filepath.Base(f.Source)
		cmdio.LogString(ctx, fmt.Sprintf("Uploading %s...", filename))

		err := uploadArtifactFile(ctx, f.Source, client)
		if err != nil {
			return err
		}

		log.Infof(ctx, "Upload succeeded")
		f.RemotePath = path.Join(uploadPath, filepath.Base(f.Source))

		// TODO: confirm if we still need to update the remote path to start with /Workspace
		wsfsBase := "/Workspace"
		remotePath := path.Join(wsfsBase, f.RemotePath)

		for _, job := range b.Config.Resources.Jobs {
			rewriteArtifactPath(b, f, job, remotePath)
		}
	}

	return nil
}

func rewriteArtifactPath(b *bundle.Bundle, f *config.ArtifactFile, job *resources.Job, remotePath string) {
	// Rewrite artifact path in job task libraries
	for i := range job.Tasks {
		task := &job.Tasks[i]
		for j := range task.Libraries {
			lib := &task.Libraries[j]
			if lib.Whl != "" && isArtifactMatchLibrary(f, lib.Whl, b) {
				lib.Whl = remotePath
			}
			if lib.Jar != "" && isArtifactMatchLibrary(f, lib.Jar, b) {
				lib.Jar = remotePath
			}
		}

		// Rewrite artifact path in job task libraries for ForEachTask
		if task.ForEachTask != nil {
			forEachTask := task.ForEachTask
			for j := range forEachTask.Task.Libraries {
				lib := &forEachTask.Task.Libraries[j]
				if lib.Whl != "" && isArtifactMatchLibrary(f, lib.Whl, b) {
					lib.Whl = remotePath
				}
				if lib.Jar != "" && isArtifactMatchLibrary(f, lib.Jar, b) {
					lib.Jar = remotePath
				}
			}
		}
	}

	// Rewrite artifact path in job environments
	for i := range job.Environments {
		env := &job.Environments[i]
		if env.Spec == nil {
			continue
		}

		for j := range env.Spec.Dependencies {
			lib := env.Spec.Dependencies[j]
			if isArtifactMatchLibrary(f, lib, b) {
				env.Spec.Dependencies[j] = remotePath
			}
		}
	}
}

func isArtifactMatchLibrary(f *config.ArtifactFile, libPath string, b *bundle.Bundle) bool {
	if !filepath.IsAbs(libPath) {
		libPath = filepath.Join(b.RootPath, libPath)
	}

	// libPath can be a glob pattern, so do the match first
	matches, err := filepath.Glob(libPath)
	if err != nil {
		return false
	}

	for _, m := range matches {
		if m == f.Source {
			return true
		}
	}

	return false
}

// Function to upload artifact file to Workspace
func uploadArtifactFile(ctx context.Context, file string, client filer.Filer) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("unable to read %s: %w", file, errors.Unwrap(err))
	}

	filename := filepath.Base(file)
	err = client.Write(ctx, filename, bytes.NewReader(raw), filer.OverwriteIfExists, filer.CreateParentDirectories)
	if err != nil {
		return fmt.Errorf("unable to import %s: %w", filename, err)
	}

	return nil
}

func getUploadBasePath(b *bundle.Bundle) (string, error) {
	artifactPath := b.Config.Workspace.ArtifactPath
	if artifactPath == "" {
		return "", fmt.Errorf("remote artifact path not configured")
	}

	return path.Join(artifactPath, ".internal"), nil
}
