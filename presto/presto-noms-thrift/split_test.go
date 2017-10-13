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
		maxBytes := int64(bytesPerRow * 10000)
		rowCount := uint64(0)
		name := &PrestoThriftSchemaTableName{schema, table}
		split := newSplit(name, offset, limit, bytesPerRow)
		batch := newBatch(split.id(), nil, maxBytes)
		for ; rowCount < limit; {
			assert.Equal(name, batch.tableName())
			assert.Equal(rowCount + offset, batch.Offset)
			rowCount += batch.Limit
			next := batch.nextBatchId(maxBytes, bytesPerRow)
			if next == nil {
				break;
			}
			batch = newBatch(split.id(), next, maxBytes)
		}
		assert.Equal(limit, rowCount, "%d != %d", limit, rowCount)

	}
	testSplit(0, 100000)
	testSplit(12345, 100000 - 12345)
}

func TestComputeRowLimit(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(uint64(50000), computeRowLimit(0, 10000000, 1, 100000))
	assert.Equal(uint64(50000), computeRowLimit(10, 10000000, 1, 100000))
	assert.Equal(uint64(90000), computeRowLimit(10000, 10000000, 1, 100000))
	assert.Equal(uint64(90000), computeRowLimit(20000, 10000000, 1, 100000))
}
