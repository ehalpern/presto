package main

import (
	"testing"

	. "prestothriftservice"

	"github.com/stretchr/testify/assert"
)

func TestSplitAndBatch(t *testing.T) {
	assert := assert.New(t)
	testSplit := func(offset, limit uint64) {
		schema, table := "test", "table"
		bytesPerRow := uint64(100)
		maxBytes := uint64(bytesPerRow * 10000)
		rowCount := uint64(0)
		name := &PrestoThriftSchemaTableName{schema, table}
		split := newSplit(name, offset, limit, bytesPerRow)
		batch := toBatch(split.id(), nil, maxBytes)
		for ; rowCount < limit; {
			assert.Equal(name, batch.tableName())
			assert.Equal(rowCount + offset, batch.Offset)
			rowCount += batch.Limit
			next := batch.nextBatchId(batch.Limit, batch.Limit * bytesPerRow, maxBytes)
			if next == nil {
				break;
			}
			batch = toBatch(split.id(), next, maxBytes)
		}
		assert.Equal(limit, rowCount, "%d != %d", limit, rowCount)

	}
	testSplit(0, 100000)
	testSplit(12345, 100000 - 12345)
}
