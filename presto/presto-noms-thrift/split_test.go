package main

import (
	"testing"

	. "prestothriftservice"

	"github.com/stretchr/testify/assert"
	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/math"
)

func TestSplitAndBatch(t *testing.T) {
	assert := assert.New(t)
	testSplit := func(offset, limit uint64) {
		tableName := &PrestoThriftSchemaTableName{"test", "table"}
		bytesPerRow := uint64(100)
		maxBytes := uint64(bytesPerRow * 10000)
		rowIdx := uint64(0)
		splitId := newSplit(tableName, offset, limit, bytesPerRow).id()
		var nextBatchId *PrestoThriftId
		for {
			batch := toBatch(splitId, nextBatchId)
			assert.Equal(tableName, batch.tableName())
			assert.Equal(rowIdx + offset, batch.Offset)
			assert.True(rowIdx < batch.Split.Limit)
			// simulate reading either maxBytes or rowsLeft
			rowsRead := math.MinUint64(batch.rowsLeft(), math.DivUint64(maxBytes, bytesPerRow))
			rowIdx += rowsRead
			nextBatchId = batch.nextBatchId(rowsRead, rowsRead * bytesPerRow, maxBytes)
			if nextBatchId == nil {
				break;
			}
		}
		assert.Equal(limit, rowIdx, "%d != %d", limit, rowIdx)

	}
	testSplit(0, 100000)
	testSplit(12345, 100000 - 12345)
}
