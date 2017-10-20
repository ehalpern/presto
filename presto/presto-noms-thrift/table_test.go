package main

import (
	"os"
	"testing"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

var dbPrefixDir = "/tmp/presto-noms-thrift"
var dbPrefix = "nbs:" + dbPrefixDir
var dbName = "test"

type row struct {
	Typebool bool
	Typedouble float64
	Typestring string
}

var rows = []row{
	{ false, 1000, "string0"},
	{ true, 1001 , "string1"},
	{ false, 1002, "string2"},
	{ true, 1003 , "string3"},
	{ false, 1004, "string4"},
	{ true, 1005 , "string5"},
}

func createTables(t *testing.T) {
	os.MkdirAll(dbPrefixDir, os.ModePerm)
	sp, err := spec.ForDatabase(dbPrefix + "/" + dbName)
	assert.NoError(t, err)
	defer sp.Close()
	db := sp.GetDatabase()

	rowMajor := marshal.MustMarshal(db, rows)
	db.Delete(db.GetDataset("types_rm"))
	_, err = db.CommitValue(db.GetDataset("types_rm"), rowMajor)
	assert.NoError(t, err)


	var bools []bool
	var doubles []float64
	var strings []string
	for _, row := range rows {
		bools = append(bools, row.Typebool)
		doubles = append(doubles, row.Typedouble)
		strings = append(strings, row.Typestring)
	}
	colMajor := types.NewStruct("Columnar", types.StructData{
		"typebool": db.WriteValue(marshal.MustMarshal(db, bools)),
		"typedouble": db.WriteValue(marshal.MustMarshal(db, doubles)),
		"typestring": db.WriteValue(marshal.MustMarshal(db, strings)),
	})
	db.Delete(db.GetDataset("types"))
	_, err = db.CommitValue(db.GetDataset("types"), colMajor)
	assert.NoError(t, err)
}

func TestGetMetaData(t *testing.T) {
	assert := assert.New(t)
	createTables(t)
	getMetadata := func(dsName string) {
		colMajor, err := getTable(dbPrefix, &PrestoThriftSchemaTableName{dbName, dsName})
		assert.NoError(err)
		metadata, err := colMajor.getMetadata()
		assert.Len(metadata.GetColumns(), 3)
		assert.NoError(err)
	}
	getMetadata("types")
	getMetadata("types_rm")
}

func TestGetRows(t *testing.T) {
	assert := assert.New(t)
	createTables(t)
	getRows := func(dsName string) {
		tableName := &PrestoThriftSchemaTableName{dbName, dsName}
		colMajor, err := getTable(dbPrefix, tableName)
		assert.NoError(err)
		splitId := newSplit(tableName, 0, 100, 100).id()
		batch := newBatch(splitId, nil, 16384)
		columns := []string {"typebool", "typedouble", "typestring"}
		blocks, rowCount, err := colMajor.getRows(batch, columns, 16384)
		assert.Len(blocks, 3)
		assert.Equal(uint64(6), rowCount, )
		// TODO: verify content
	}
	getRows("types")
	getRows("types_rm")
}
