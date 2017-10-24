package main

var config = struct {
	dbPrefix         string
	minBytesPerSplit uint64
	workerCount      uint64
	splitsPerWorker  uint64
}{
	"nbs:/tmp/presto-noms",
	64 * 1024,
	1,
	4,
}
