package resources

import (
	"github.com/databricks/cli/bundle/config/paths"
	"github.com/databricks/databricks-sdk-go/marshal"
	"github.com/databricks/databricks-sdk-go/service/ml"
)

type MlflowExperiment struct {
	Permissions []Permission `json:"permissions,omitempty"`

	paths.Paths

	*ml.Experiment
}

func (s *MlflowExperiment) GetURL(workspace_host string) string {
	return workspace_host + "/ml/experiments/" + s.ExperimentId
}

func (s *MlflowExperiment) UnmarshalJSON(b []byte) error {
	return marshal.Unmarshal(b, s)
}

func (s MlflowExperiment) MarshalJSON() ([]byte, error) {
	return marshal.Marshal(s)
}
