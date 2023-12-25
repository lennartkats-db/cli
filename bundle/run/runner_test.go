package run

import (
	"testing"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/stretchr/testify/assert"
)

func TestFindNoResources(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Resources: config.Resources{},
		},
	}

	_, err := find(b, "foo")
	assert.ErrorContains(t, err, "bundle defines no resources")
}

func TestFindSingleArg(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"foo": {},
				},
			},
		},
	}

	_, err := find(b, "foo")
	assert.NoError(t, err)
}

func TestFindSingleArgNotFound(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"foo": {},
				},
			},
		},
	}

	_, err := find(b, "bar")
	assert.ErrorContains(t, err, "no such resource: bar")
}

func TestFindSingleArgAmbiguous(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"key": {},
				},
				Pipelines: map[string]*resources.Pipeline{
					"key": {},
				},
			},
		},
	}

	_, err := find(b, "key")
	assert.ErrorContains(t, err, "ambiguous: ")
}

func TestFindSingleArgWithType(t *testing.T) {
	b := &bundle.Bundle{
		Config: config.Root{
			Resources: config.Resources{
				Jobs: map[string]*resources.Job{
					"key": {},
				},
			},
		},
	}

	_, err := find(b, "jobs.key")
	assert.NoError(t, err)
}
