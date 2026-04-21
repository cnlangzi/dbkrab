package replay

// ReplayResult contains replay statistics.
type ReplayResult struct {
	TotalLSNs     int
	TotalChanges  int
	ProcessedLSNs int
	FailedLSNs    int
}

// ProgressCallback is called after each LSN is processed.
type ProgressCallback func(processed int, total int)

// LSNReplayResult contains metrics from processing a single LSN.
type LSNReplayResult struct {
	FetchedRows int
	TxCount     int
	DLQCount    int
}