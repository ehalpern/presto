package main

import (
	"encoding/json"
	"log"
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

// Naively estimate the number of rows required to fill the buffer with up to |maxBytes|
//
// To do this, we estimate the number of rows required for |maxBytes| using the
// average bytes/row read so far. We then compute a confidence measure based on
// the number of rows already read between 0.8 and 0.1. 0.8 is the low bound of
// confidence and means we leave room for 80% error. 0.1 is the high bound of
// confidence and means we leave room for 10% error.
func computeRowLimit(rowsRead uint64, totalRows uint64, estBytesPerRow uint64, maxBytes int64) uint64 {
	// rowsRead == 0 means this is the first batch and we've estimated bytes/row using table.estimateRowSize
	confidence := math.Min(.8, math.Max(.1, 10.0 / math.Sqrt(math.Max(1, float64(rowsRead)))))
	estimatedRows := float64(maxBytes) / float64(estBytesPerRow)
	limit := estimatedRows * (1.0 - confidence)
	limit = math.Min(limit, float64(totalRows - rowsRead))
	log.Printf("rowsRead: %v, totalRows: %v", rowsRead, totalRows)
	log.Printf("New limit: %v (assuming %v bytes/row, targeting %v bytes with %v confidence)",
		limit, estBytesPerRow, maxBytes, confidence)
	return uint64(math.Floor(limit + .5))
}

