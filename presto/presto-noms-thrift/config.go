package main

var config = struct {
	dbPrefix string
	minBytesPerSplit uint64
	nodeCount uint64
}{
	"nbs:/tmp/presto-noms",
	64 * 1024,
	3,
}
