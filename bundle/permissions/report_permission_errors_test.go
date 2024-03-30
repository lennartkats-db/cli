package permissions

import (
	"context"
	"errors"
	"testing"

	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/stretchr/testify/require"
)

func TestApplySuccess(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_MANAGE", UserName: "testuser@databricks.com"},
	})

	diags := ReportPermissionErrors().Apply(context.Background(), b)
	require.NoError(t, diags.Error())
}

func TestApplyFail(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_VIEW", UserName: "testuser@databricks.com"},
	})

	diags := ReportPermissionErrors().Apply(context.Background(), b)
	require.ErrorContains(t, diags.Error(), "necessary permissions to deploy")
}

func TestPermissionDeniedWithPermission(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_MANAGE", GroupName: "testgroup"},
	})

	diags := ReportPermissionDenied(context.Background(), b, "testpath")
	require.ErrorContains(t, diags.Error(), "access denied to update permissions")
}

func TestPermissionDeniedWithoutPermission(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_VIEW", UserName: "testuser@databricks.com"},
	})

	diags := ReportPermissionDenied(context.Background(), b, "testpath")
	require.ErrorContains(t, diags.Error(), "necessary permissions to deploy")
}

func TestPermissionDeniedNilPermission(t *testing.T) {
	b := mockBundle(nil)

	diags := ReportPermissionDenied(context.Background(), b, "testpath")
	require.ErrorContains(t, diags.Error(), "necessary permissions to deploy")
}

func TestFindOtherOwners(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_MANAGE", GroupName: "testgroup"},
		{Level: "CAN_MANAGE", UserName: "alice@databricks.com"},
	})

	diags := ReportPermissionDenied(context.Background(), b, "testpath")
	require.ErrorContains(t, diags.Error(), "include: alice@databricks.com")
}

func TestReportTerraformError1(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_MANAGE", UserName: "alice@databricks.com"},
	})
	err := TryReportTerraformPermissionError(b, errors.New(`Error: terraform apply: exit status 1

Error: cannot update permissions: ...

	with databricks_pipeline.my_project_pipeline,
	on bundle.tf.json line 39, in resource.databricks_pipeline.my_project_pipeline:
	39:       }`)).Error()
	require.ErrorContains(t, err, "EPERM3")
	require.ErrorContains(t, err, "pipeline")
}

func TestReportTerraformError2(t *testing.T) {
	b := mockBundle([]resources.Permission{
		{Level: "CAN_MANAGE", UserName: "alice@databricks.com"},
	})
	err := TryReportTerraformPermissionError(b, errors.New(`Error: terraform apply: exit status 1

Error: cannot read pipeline: User xyz does not have View permissions on pipeline 4521dbb6-42aa-418c-b94d-b5f4859a3454.

	with databricks_pipeline.my_project_pipeline,
	on bundle.tf.json line 39, in resource.databricks_pipeline.my_project_pipeline:
	39:       }`)).Error()
	require.ErrorContains(t, err, "EPERM3")
	require.ErrorContains(t, err, "pipeline")
}

func mockBundle(permissions []resources.Permission) *bundle.Bundle {
	return &bundle.Bundle{
		Config: config.Root{
			Workspace: config.Workspace{
				CurrentUser: &config.User{
					User: &iam.User{
						UserName: "testuser@databricks.com",
						Groups: []iam.ComplexValue{
							{Display: "testgroup"},
						},
					},
				},
			},
			Permissions: permissions,
		},
	}
}
