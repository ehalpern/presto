package main

import (
	"testing"

	. "prestothriftservice"

	"github.com/stretchr/testify/assert"
)

func TestSplitAndBatch(t *testing.T) {
	assert := assert.New(t)
	schema, table := "test", "table"
	offset, limit := uint64(1000), uint64(10005)
	bytesPerRow := uint64(100)
	maxBytes := int64(bytesPerRow * 10000)
	rowCount := offset
	name := &PrestoThriftSchemaTableName{schema, table}
	split := newSplit(name, offset, limit, bytesPerRow)
	batch := newBatch(split.id(), nil, maxBytes)
	for ; rowCount < limit; {
		assert.Equal(name, batch.tableName())
		assert.Equal(rowCount, batch.Offset)
		rowCount += batch.Limit
		next := batch.nextBatchId(maxBytes, bytesPerRow)
		if next == nil {
			break;
		}
		batch = newBatch(split.id(), next, maxBytes)
	}
	assert.Equal(limit, rowCount, "%d != %d", limit, rowCount)
}

func TestComputeRowLimit(t *testing.T) {
	assert := assert.New(t)

	// Read ~ 1/10 or rows based on initial estimate
	assert.Equal(uint64(10), computeRowLimit(0, 1000, 1, 100))

	// Read ~ 1/10 or rows based on initial estimate
	assert.Equal(uint64(20), computeRowLimit(20, 1000, 1, 100))

	// Read ~ 1/10 or rows based on initial estimate
	assert.Equal(uint64(90), computeRowLimit(100, 1000, 1, 100))

	// Read ~ 1/10 or rows based on initial estimate
	assert.Equal(uint64(90), computeRowLimit(500, 1000, 1, 100))
}
