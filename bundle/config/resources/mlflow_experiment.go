package resources

import (
	"github.com/databricks/cli/bundle/config/paths"
	"github.com/databricks/databricks-sdk-go/marshal"
	"github.com/databricks/databricks-sdk-go/service/ml"
)

type MlflowExperiment struct {
	ID             string         `json:"id,omitempty" bundle:"readonly"`
	Permissions    []Permission   `json:"permissions,omitempty"`
	ModifiedStatus ModifiedStatus `json:"modified_status,omitempty" bundle:"internal"`

	paths.Paths

	*ml.Experiment
}

func (s *MlflowExperiment) UnmarshalJSON(b []byte) error {
	return marshal.Unmarshal(b, s)
}

func (s MlflowExperiment) MarshalJSON() ([]byte, error) {
	return marshal.Marshal(s)
}
