package main

import (
	"encoding/json"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/math"

)

type Split struct {
	Schema         string `json:`
	Table          string `json:`
	Offset         uint64 `json:`
	Limit          uint64 `json:`
	EstBytesPerRow uint64 `json:`
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
	Limit      uint64 `json:`
	EstBytesPerRow uint64 `json:`
}

// Return batch given |split| and |batchToken|
// If |batchToken| is nil, create the first batch of the split
// If |batchToken| is non-nil, simply unmarshal the batchId
func toBatch(splitId *PrestoThriftId, batchToken *PrestoThriftId, maxBytes uint64) *Batch {
	var b Batch
	if batchToken != nil {
		d.PanicIfError(json.Unmarshal(batchToken.GetID(), &b))
	} else {
		s := splitFromId(splitId)
		limit := estimateRowLimit(0, s.Limit, s.EstBytesPerRow, maxBytes)
		b = Batch{*s, s.Offset, limit, s.EstBytesPerRow}
		d.Chk.True(b.Limit > 0)
		d.Chk.True(b.Offset >= b.Split.Offset)
		d.Chk.True(b.totalRowsRead() <= b.Split.Limit, "%d !<= %d", b.totalRowsRead(), b.Split.Limit)
	}
	return &b
}

func (b *Batch) totalRowsRead() uint64 {
	return b.Offset + b.Limit - b.Split.Offset
}

func (b *Batch) nextBatch(rowsRead, bytesRead, maxBytes uint64) *Batch {
	newOffset := b.Offset + rowsRead
	if newOffset >= b.Split.Limit {
		return nil
	}
	totalRowsRead := newOffset - b.Split.Offset
	avgBytesPerRow := math.DivUint64(bytesRead, rowsRead)
	newLimit := estimateRowLimit(totalRowsRead, b.Split.Limit, avgBytesPerRow, maxBytes)
	return &Batch{
		b.Split,
		newOffset, newLimit,
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

func estimateRowLimit(rowsRead uint64, totalRows uint64, avgBytesPerRow uint64, maxBytes uint64) uint64 {
	rowCount := math.DivUint64(maxBytes, avgBytesPerRow)
	return math.MinUint64(rowCount, totalRows - rowsRead)
}

