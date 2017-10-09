package main

import (
	"encoding/json"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
)

type Batch struct {
	Schema     string `json:`
	Table      string `json:`
	Offset     uint64 `json:`
	Limit      uint64 `json:`
	TotalLimit uint64 `json:`
}

// Create batch from |split| or |batchToken|
// If |batchToken| is nil, create the first batch of the split
// If |batchToken| is non-nil, simply unmarshal the batchId
func newBatch(splitId *PrestoThriftId, batchToken *PrestoThriftNullableToken) *Batch {
	var b Batch
	if batchToken.IsSetToken() {
		d.PanicIfError(json.Unmarshal(batchToken.GetToken().GetID(), &b))
	} else {
		s := splitFromId(splitId)
		b = Batch{
			s.Schema, s.Table,
			s.Offset, minUint64(config.maxBatchSize, s.Limit),
			s.Limit,
		}
	}
	return &b
}

func (b *Batch) nextBatchToken() *PrestoThriftId {
	if b.Offset + b.Limit == b.TotalLimit {
		return nil
	}
	next := Batch{
		b.Schema, b.Table,
		b.Offset + b.Limit, minUint64(config.maxBatchSize, b.TotalLimit),
		b.TotalLimit,
	}
	return next.id()
}


func (b *Batch) id() *PrestoThriftId {
	bytes, err := json.Marshal(b)
	d.PanicIfError(err)
	return &PrestoThriftId{ bytes }
}

func (s *Batch) tableName() *PrestoThriftSchemaTableName {
	return &PrestoThriftSchemaTableName{ s.Schema, s.Table}
}
