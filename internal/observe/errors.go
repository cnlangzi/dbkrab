package observe

import "errors"

var (
	// ErrLogsClosed indicates the logs database is closed
	ErrLogsClosed = errors.New("logs database is closed")
	// ErrPullIDRequired indicates pull_id is required for skill/sink logs
	ErrPullIDRequired = errors.New("pull_id is required")
	// ErrInvalidStatus indicates an invalid status value
	ErrInvalidPullStatus = errors.New("invalid pull status: must be SUCCESS, PARTIAL, or FAILED")
	ErrInvalidSkillStatus = errors.New("invalid skill status: must be SKIP, EXECUTED, or ERROR")
	ErrInvalidSinkStatus = errors.New("invalid sink status: must be SUCCESS or ERROR")
)