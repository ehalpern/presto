package main

import (
	"encoding/json"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/math"

	"log"
)

type Split struct {
	Schema         string `json:`
	Table          string `json:`
	Offset         uint64 `json:`
	Limit          uint64 `json:`
	EstBytesPerRow uint64 `json:`
}

func determineSplits(
	tableName *PrestoThriftSchemaTableName,
	rowCount, estBytesPerRow, minBytesPerSplit, maxSplits uint64) (splits []*Split) {
	if estBytesPerRow == 0 {
		log.Printf("Using single split to count rows")
		return append(splits, newSplit(tableName, uint64(0), uint64(rowCount), uint64(0)))
	}
	minRowsPerSplit := minBytesPerSplit / estBytesPerRow
	maxSplits = math.MinUint64(config.workerCount * config.splitsPerWorker, uint64(maxSplits))
	splitCount := math.MaxUint64(1, math.MinUint64(rowCount/minRowsPerSplit, maxSplits))
	rowsPerSplit := rowCount / splitCount
	remainder := rowCount % rowsPerSplit
	for i := uint64(0); i < splitCount; i++ {
		limit := rowsPerSplit
		if i+1 == splitCount && remainder > 0 {
			limit = remainder
		}
		split := newSplit(tableName, i*rowsPerSplit, limit, estBytesPerRow)
		splits = append(splits, split)
	}
	log.Printf("Splitting query into %d splits of %d rows of %d estimated bytes", splitCount, rowsPerSplit, estBytesPerRow)
	return splits
}

func newSplit(table *PrestoThriftSchemaTableName, offset uint64, limit uint64, bytesPerRow uint64) *Split {
	d.Chk.True(limit > 0)
	return &Split{
		table.SchemaName, table.TableName, offset, limit, bytesPerRow,
	}
}

func splitFromId(id *PrestoThriftId) *Split {
	var s Split
	d.PanicIfError(json.Unmarshal(id.GetID(), &s))
	return &s
}

func (s *Split) id() *PrestoThriftId {
	bytes, err := json.Marshal(s)
	d.PanicIfError(err)
	return &PrestoThriftId{ bytes }
}

func (s *Split) tableName() *PrestoThriftSchemaTableName {
	return &PrestoThriftSchemaTableName{ s.Schema, s.Table}
}

type Batch struct {
	Split 		Split
	Offset     uint64 `json:`
	EstBytesPerRow uint64 `json:`
}

// Return batch given |split| and |batchToken|
// If |batchToken| is nil, create the first batch of the split
// If |batchToken| is non-nil, simply unmarshal the batchId
func toBatch(splitId *PrestoThriftId, batchToken *PrestoThriftId) *Batch {
	var b Batch
	if batchToken != nil {
		d.PanicIfError(json.Unmarshal(batchToken.GetID(), &b))
	} else {
		s := splitFromId(splitId)
		b = Batch{*s, s.Offset, s.EstBytesPerRow}
		d.Chk.True(b.Offset >= b.Split.Offset)
	}
	return &b
}

func (b *Batch) rowsLeft() uint64 {
	return b.Split.Limit - (b.Offset - b.Split.Offset)
}

func (b *Batch) nextBatch(rowsRead, bytesRead, maxBytes uint64) *Batch {
	newOffset := b.Offset + rowsRead
	if newOffset - b.Split.Offset >= b.Split.Limit {
		return nil
	}
	avgBytesPerRow := math.DivUint64(bytesRead, rowsRead)
	return &Batch{
		b.Split,
		newOffset,
		avgBytesPerRow,
	}
}

func (b *Batch) nextBatchId(rowsRead, bytesRead, maxBytes uint64) *PrestoThriftId {
	next := b.nextBatch(rowsRead, bytesRead, maxBytes)
	if next == nil {
		return nil
	}
	return next.id()
}

func (b *Batch) id() *PrestoThriftId {
	bytes, err := json.Marshal(b)
	d.PanicIfError(err)
	return &PrestoThriftId{ bytes }
}

func (b *Batch) tableName() *PrestoThriftSchemaTableName {
	return &PrestoThriftSchemaTableName{ b.Split.Schema, b.Split.Table}
}

