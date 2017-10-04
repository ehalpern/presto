package main

import (
	"encoding/json"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
)

type Split struct {
	Schema     string  `json:`
	Table      string  `json:`
	Offset     uint64  `json:`
	Limit      uint64  `json:`
}

func newSplit(table *PrestoThriftSchemaTableName, offset uint64, limit uint64) *Split {
	return &Split{
		table.SchemaName, table.TableName, offset, limit,
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
