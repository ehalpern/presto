package main

var config = struct {
	dbPrefix string
	maxBatchSize uint64
	minRowsPerSplit uint64
}{
	"nbs:/tmp/presto-noms",
	1000000,
	1000000,
}
