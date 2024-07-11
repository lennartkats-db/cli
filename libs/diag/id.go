package diag

type ID string

// For select diagnostic messages we use IDs to identify them
// for support or tooling purposes.
// It is a non-goal to have an exhaustive list of IDs.
const (
	// General errors

	ConfigurationError   ID = "ECONFIG"
	InternalError        ID = "EINTERNAL"
	ArtifactError        ID = "EARTIFACT"
	IOError              ID = "EIO"
	AbortedError         ID = "EABORTED"
	ConfigurationWarning ID = "WCONFIG"
	DBRVersionError      ID = "EDBRVER"

	// Errors related to specific components or functionality

	PathPermissionDenied        ID = "EPERM1"
	ResourcePermissionDenied    ID = "EPERM2"
	CannotChangePathPermissions ID = "EPERM3"
	RunAsDenied                 ID = "EPERM4"
	PermissionNotIncluded       ID = "EPERM5"
	BuildError                  ID = "EBUILD"
	EnvironmentError            ID = "EENV"
	WorkspaceClientError        ID = "EWSCLIENT"
	GitError                    ID = "EGIT"
	PyDABsError                 ID = "EPYDABS"
	PyDABsMutatorError          ID = "EMUTATOR"
	TargetModeError             ID = "ETARGETMODE"
	ReferenceError              ID = "EREFERENCE"
	VariableError               ID = "EVAR"
	SyncError                   ID = "ESYNC"
	TrampolineError             ID = "ETRAMPOLINE"
	CLIVersionError             ID = "ECLIVER"
	LockError                   ID = "ELOCK"
	StateError                  ID = "ESTATE"
	TerraformSetupError         ID = "ETERSETUP"
	TerraformError              ID = "ETER"
	RunError                    ID = "ERUN"
	ScriptError                 ID = "ESCRIPT"
)
