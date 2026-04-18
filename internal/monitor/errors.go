package monitor

import "errors"

var (
	// ErrLogsClosed indicates the logs database is closed
	ErrLogsClosed = errors.New("logs database is closed")
	// ErrBatchIDRequired indicates batch_id is required for skill/sink logs
	ErrBatchIDRequired = errors.New("batch_id is required")
	// ErrInvalidStatus indicates an invalid status value
	ErrInvalidPullStatus  = errors.New("invalid pull status: must be SUCCESS, PARTIAL, or FAILED")
	ErrInvalidSkillStatus = errors.New("invalid skill status: must be SKIP, EXECUTED, or ERROR")
	ErrInvalidSinkStatus  = errors.New("invalid sink status: must be SUCCESS or ERROR")
)
