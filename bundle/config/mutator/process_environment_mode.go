package mutator

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/databricks/databricks-sdk-go/service/ml"
)

type processEnvironmentMode struct{}

const developmentConcurrentRuns = 4

func ProcessEnvironmentMode() bundle.Mutator {
	return &processEnvironmentMode{}
}

func (m *processEnvironmentMode) Name() string {
	return "ProcessEnvironmentMode"
}

// Mark all resources as being for 'development' purposes, i.e.
// changing their their name, adding tags, and (in the future)
// marking them as 'hidden' in the UI.
func transformDevelopmentMode(b *bundle.Bundle) error {
	r := b.Config.Resources

	prefix := "[dev " + b.Config.Workspace.CurrentUser.ShortName + "] "

	for i := range r.Jobs {
		r.Jobs[i].Name = prefix + r.Jobs[i].Name
		if r.Jobs[i].Tags == nil {
			r.Jobs[i].Tags = make(map[string]string)
		}
		r.Jobs[i].Tags["dev"] = b.Config.Workspace.CurrentUser.DisplayName
		if r.Jobs[i].MaxConcurrentRuns == 0 {
			r.Jobs[i].MaxConcurrentRuns = developmentConcurrentRuns
		}
		if r.Jobs[i].Schedule != nil {
			r.Jobs[i].Schedule.PauseStatus = jobs.PauseStatusPaused
		}
		if r.Jobs[i].Continuous != nil {
			r.Jobs[i].Continuous.PauseStatus = jobs.PauseStatusPaused
		}
		if r.Jobs[i].Trigger != nil {
			r.Jobs[i].Trigger.PauseStatus = jobs.PauseStatusPaused
		}
	}

	for i := range r.Pipelines {
		r.Pipelines[i].Name = prefix + r.Pipelines[i].Name
		r.Pipelines[i].Development = true
		// (pipelines don't yet support tags)
	}

	for i := range r.Models {
		r.Models[i].Name = prefix + r.Models[i].Name
		r.Models[i].Tags = append(r.Models[i].Tags, ml.ModelTag{Key: "dev", Value: ""})
	}

	for i := range r.Experiments {
		filepath := r.Experiments[i].Name
		dir := path.Dir(filepath)
		base := path.Base(filepath)
		if dir == "." {
			r.Experiments[i].Name = prefix + base
		} else {
			r.Experiments[i].Name = dir + "/" + prefix + base
		}
		r.Experiments[i].Tags = append(r.Experiments[i].Tags, ml.ExperimentTag{Key: "dev", Value: b.Config.Workspace.CurrentUser.DisplayName})
	}

	return nil
}

func validateDevelopmentMode(b *bundle.Bundle) error {
	if path := findIncorrectPath(b, config.Development); path != "" {
		return fmt.Errorf("%s must start with '~/' or contain the current username when using 'mode: development'", path)
	}
	return nil
}

func findIncorrectPath(b *bundle.Bundle, mode config.Mode) string {
	username := b.Config.Workspace.CurrentUser.UserName
	containsExpected := true
	if mode == config.Production {
		containsExpected = false
	}

	if strings.Contains(b.Config.Workspace.RootPath, username) != containsExpected && b.Config.Workspace.RootPath != "" {
		return "root_path"
	}
	if strings.Contains(b.Config.Workspace.StatePath, username) != containsExpected {
		return "state_path"
	}
	if strings.Contains(b.Config.Workspace.FilesPath, username) != containsExpected {
		return "files_path"
	}
	if strings.Contains(b.Config.Workspace.ArtifactsPath, username) != containsExpected {
		return "artifacts_path"
	}
	return ""
}

func validateProductionMode(ctx context.Context, b *bundle.Bundle, isPrincipalUsed bool) error {
	r := b.Config.Resources
	for i := range r.Pipelines {
		if r.Pipelines[i].Development {
			return fmt.Errorf("environment with 'mode: production' cannot specify a pipeline with 'development: true'")
		}
	}

	if !isPrincipalUsed {
		if path := findIncorrectPath(b, config.Production); path != "" {
			message := "%s must not contain the current username when using 'mode: production' without a service principal"
			if path == "root_path" {
				return fmt.Errorf(message+"\n  tip: set workspace.root_path to a shared path such as /Shared/.bundle/${bundle.name}/${bundle.environment}", path)
			} else {
				return fmt.Errorf(message, path)
			}
		}

		if !arePermissionsSetExplicitly(r) {
			return fmt.Errorf("permissions and run_as must be set when using 'mode_production' without a service principals")
		}
	}
	return nil
}

// Determines whether a service principal identity is used to run the CLI.
func isServicePrincipalUsed(ctx context.Context, b *bundle.Bundle) (bool, error) {
	ws := b.WorkspaceClient()

	_, err := ws.ServicePrincipals.GetById(ctx, b.Config.Workspace.CurrentUser.Id)
	if err != nil {
		apiError, ok := err.(*apierr.APIError)
		if ok && apiError.StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	return false, nil
}

// Determines whether permissions and run_as are explicitly set for all resources.
// We do this in a best-effort fashion; we may not actually test all resources,
// as we expect customers to use the top-level 'permissions' and 'run_as' fields.
// We'd rather not check for those specific fields though, as customers might
// set specific permissions instead!
func arePermissionsSetExplicitly(r config.Resources) bool {
	for i := range r.Pipelines {
		if r.Pipelines[i].Permissions == nil {
			return false
		}
	}

	for i := range r.Jobs {
		if r.Jobs[i].Permissions == nil {
			return false
		}
		if r.Jobs[i].RunAs == nil {
			return false
		}
	}
	return true
}

func (m *processEnvironmentMode) Apply(ctx context.Context, b *bundle.Bundle) error {
	switch b.Config.Bundle.Mode {
	case config.Development:
		err := validateDevelopmentMode(b)
		if err != nil {
			return err
		}
		return transformDevelopmentMode(b)
	case config.Production:
		isPrincipal, err := isServicePrincipalUsed(ctx, b)
		if err != nil {
			return err
		}
		return validateProductionMode(ctx, b, isPrincipal)
	case "":
		// No action
	default:
		return fmt.Errorf("unsupported value specified for 'mode': %s", b.Config.Bundle.Mode)
	}

	return nil
}
