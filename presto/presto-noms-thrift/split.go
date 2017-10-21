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
	Schema     string `json:`
	Table      string `json:`
	Offset     uint64 `json:`
	Limit      uint64 `json:`
	InitialOffset uint64 `json:`
	TotalLimit uint64 `json:`
	EstBytesPerRow uint64 `json`
}

// Return batch given |split| and |batchToken|
// If |batchToken| is nil, create the first batch of the split
// If |batchToken| is non-nil, simply unmarshal the batchId
func toBatch(splitId *PrestoThriftId, batchToken *PrestoThriftId, maxBytes int64) *Batch {
	var b Batch
	if batchToken != nil {
		d.PanicIfError(json.Unmarshal(batchToken.GetID(), &b))
	} else {
		s := splitFromId(splitId)
		limit := computeRowLimit(0, s.Limit, s.EstBytesPerRow, maxBytes)
		b = Batch{
			s.Schema, s.Table,
			s.Offset, limit,
			s.Offset,s.Limit,
			s.EstBytesPerRow,
		}
		d.Chk.True(b.Limit > 0)
		d.Chk.True(b.Offset >= b.InitialOffset)
		d.Chk.True(b.totalRowsRead() <= b.TotalLimit, "%d !<= %d", b.totalRowsRead(), b.TotalLimit)
	}
	return &b
}

func (b *Batch) totalRowsRead() uint64 {
	return b.Offset + b.Limit - b.InitialOffset
}

func (b *Batch) nextBatchId(maxBytes int64, actualBytesPerRow uint64) *PrestoThriftId {
	if b.totalRowsRead() == b.TotalLimit {
		return nil
	}
	newLimit := computeRowLimit(b.totalRowsRead(), b.TotalLimit, actualBytesPerRow, maxBytes)
	return (&Batch{
		b.Schema, b.Table,
		b.Offset + b.Limit, uint64(newLimit),
		b.InitialOffset,b.TotalLimit,
		actualBytesPerRow,
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

// Estimate the number of rows required to fill the buffer with |maxBytes|
// Then adjust down based on the the number of rows that inform the estimate.
func computeRowLimit(rowsRead uint64, totalRows uint64, avgBytesPerRow uint64, maxBytes int64) uint64 {
	// rowsRead == 0 means this is the first batch and we've estimated bytes/row using table.estimateRowSize

	// Compute confidence between .5 and .05 based on number of rows read
	// Reach .05 at 10000
	confidence := 1.0 - math.Min(.5, math.Max(.05, 5.0 / math.Sqrt(math.Max(1, float64(rowsRead)))))

	// Approach maxBytes as confidence increase
	targetBytes := float64(maxBytes) * confidence

	rowCount := targetBytes / float64(avgBytesPerRow)
	rowCount = math.Min(rowCount, float64(totalRows - rowsRead))
	return uint64(round(rowCount))
}

