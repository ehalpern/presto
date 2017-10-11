package main

import (
	"encoding/json"
	"math"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
)

type Split struct {
	Schema         string `json:`
	Table          string `json:`
	Offset         uint64 `json:`
	Limit          uint64 `json:`
	EstBytesPerRow uint64 `json:`
}

func newSplit(table *PrestoThriftSchemaTableName, offset uint64, limit uint64, bytesPerRow uint64) *Split {
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
	Schema     string `json:`
	Table      string `json:`
	Offset     uint64 `json:`
	Limit      uint64 `json:`
	InitialOffset uint64 `json:`
	TotalLimit uint64 `json:`
}

// Create batch from |split| or |batchToken|
// If |batchToken| is nil, create the first batch of the split
// If |batchToken| is non-nil, simply unmarshal the batchId
func newBatch(splitId *PrestoThriftId, batchToken *PrestoThriftId, maxBytes int64) *Batch {
	var b Batch
	if batchToken != nil {
		d.PanicIfError(json.Unmarshal(batchToken.GetID(), &b))
	} else {
		s := splitFromId(splitId)
		limit := computeRowLimit(0, s.Limit - s.Offset, s.EstBytesPerRow, maxBytes)
		b = Batch{
			s.Schema, s.Table,
			s.Offset, limit,
			s.Offset,s.Limit,
		}
	}
	return &b
}

func (b *Batch) nextBatchId(maxBytes int64, estBytesPerRow uint64) *PrestoThriftId {
	newOffset := b.Offset + b.Limit
	if newOffset == b.TotalLimit {
		return nil
	}
	newLimit := computeRowLimit(newOffset - b.InitialOffset, b.TotalLimit - b.InitialOffset, estBytesPerRow, maxBytes)
	return (&Batch{
		b.Schema, b.Table,
		newOffset, uint64(newLimit),
		b.InitialOffset,b.TotalLimit,
	}).id()
}

func (b *Batch) id() *PrestoThriftId {
	bytes, err := json.Marshal(b)
	d.PanicIfError(err)
	return &PrestoThriftId{ bytes }
}

func (s *Batch) tableName() *PrestoThriftSchemaTableName {
	return &PrestoThriftSchemaTableName{ s.Schema, s.Table}
}

// The goal is to read up to maxBytes per batch without going over. To do this, we
// approximate the average bytes/row based on the rows that have already been read.
// Our confidence in this approximation increases with the number of rows read.
// The next batch limit is computed by taking the current bytes/row estimate divided
// by the maxBytes and reduced by the confidence factor. At the lowest confidence (less
// than 1% read), we only read only 1/10th of the rows estimated to reach maxBytes.
// At the highest confidence (10% read), we read 9/10ths of the rows estimated to reach
// maxBytes.
func computeRowLimit(rowsRead uint64, totalRows uint64, estBytesPerRow uint64, maxBytes int64) uint64 {
	confidence := float64(rowsRead)/float64(totalRows) * 10
	confidence = math.Min(.9, math.Max(.1, confidence))
	estimatedRows := float64(maxBytes) / float64(estBytesPerRow)
	newLimit := math.Max(1, estimatedRows * confidence)
	return uint64(math.Min(newLimit, float64(totalRows - rowsRead)))
}

