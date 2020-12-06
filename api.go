package consistenthashing

type ContinuesJob struct {
	Id   uint64
	Data []byte
}

type JobResult struct {
	Id          uint64
	ProcessedBy string
}

type TerminateSignal struct{}
