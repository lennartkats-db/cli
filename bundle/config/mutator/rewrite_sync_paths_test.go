package mutator_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/mutator"
	"github.com/databricks/cli/bundle/internal/bundletest"
	"github.com/stretchr/testify/assert"
)

func TestRewriteSyncPathsRelative(t *testing.T) {
	b := &bundle.Bundle{
		RootPath: ".",
		Config: config.Root{
			Sync: config.Sync{
				Paths: []string{
					".",
					"../common",
				},
				Include: []string{
					"foo",
					"bar",
				},
				Exclude: []string{
					"baz",
					"qux",
				},
			},
		},
	}

	bundletest.SetLocation(b, "sync.paths[0]", "./databricks.yml")
	bundletest.SetLocation(b, "sync.paths[1]", "./databricks.yml")
	bundletest.SetLocation(b, "sync.include[0]", "./file.yml")
	bundletest.SetLocation(b, "sync.include[1]", "./a/file.yml")
	bundletest.SetLocation(b, "sync.exclude[0]", "./a/b/file.yml")
	bundletest.SetLocation(b, "sync.exclude[1]", "./a/b/c/file.yml")

	diags := bundle.Apply(context.Background(), b, mutator.RewriteSyncPaths())
	assert.NoError(t, diags.Error())

	assert.Equal(t, filepath.Clean("."), b.Config.Sync.Paths[0])
	assert.Equal(t, filepath.Clean("../common"), b.Config.Sync.Paths[1])
	assert.Equal(t, filepath.Clean("foo"), b.Config.Sync.Include[0])
	assert.Equal(t, filepath.Clean("a/bar"), b.Config.Sync.Include[1])
	assert.Equal(t, filepath.Clean("a/b/baz"), b.Config.Sync.Exclude[0])
	assert.Equal(t, filepath.Clean("a/b/c/qux"), b.Config.Sync.Exclude[1])
}

func TestRewriteSyncPathsAbsolute(t *testing.T) {
	b := &bundle.Bundle{
		RootPath: "/tmp/dir",
		Config: config.Root{
			Sync: config.Sync{
				Paths: []string{
					".",
					"../common",
				},
				Include: []string{
					"foo",
					"bar",
				},
				Exclude: []string{
					"baz",
					"qux",
				},
			},
		},
	}

	bundletest.SetLocation(b, "sync.paths[0]", "/tmp/dir/databricks.yml")
	bundletest.SetLocation(b, "sync.paths[1]", "/tmp/dir/databricks.yml")
	bundletest.SetLocation(b, "sync.include[0]", "/tmp/dir/file.yml")
	bundletest.SetLocation(b, "sync.include[1]", "/tmp/dir/a/file.yml")
	bundletest.SetLocation(b, "sync.exclude[0]", "/tmp/dir/a/b/file.yml")
	bundletest.SetLocation(b, "sync.exclude[1]", "/tmp/dir/a/b/c/file.yml")

	diags := bundle.Apply(context.Background(), b, mutator.RewriteSyncPaths())
	assert.NoError(t, diags.Error())

	assert.Equal(t, filepath.Clean("."), b.Config.Sync.Paths[0])
	assert.Equal(t, filepath.Clean("../common"), b.Config.Sync.Paths[1])
	assert.Equal(t, filepath.Clean("foo"), b.Config.Sync.Include[0])
	assert.Equal(t, filepath.Clean("a/bar"), b.Config.Sync.Include[1])
	assert.Equal(t, filepath.Clean("a/b/baz"), b.Config.Sync.Exclude[0])
	assert.Equal(t, filepath.Clean("a/b/c/qux"), b.Config.Sync.Exclude[1])
}

func TestRewriteSyncPathsErrorPaths(t *testing.T) {
	t.Run("no sync block", func(t *testing.T) {
		b := &bundle.Bundle{
			RootPath: ".",
		}

		diags := bundle.Apply(context.Background(), b, mutator.RewriteSyncPaths())
		assert.NoError(t, diags.Error())
	})

	t.Run("empty include/exclude blocks", func(t *testing.T) {
		b := &bundle.Bundle{
			RootPath: ".",
			Config: config.Root{
				Sync: config.Sync{
					Include: []string{},
					Exclude: []string{},
				},
			},
		}

		diags := bundle.Apply(context.Background(), b, mutator.RewriteSyncPaths())
		assert.NoError(t, diags.Error())
	})
}
