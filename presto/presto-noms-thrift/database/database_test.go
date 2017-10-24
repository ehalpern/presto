package database

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
	"github.com/attic-labs/noms/go/spec"
)

const (
	testDS = "testDS"
	testValue = types.String("testValue")
	nbsDB = "nbs:/tmp/presto-noms-thrift/test"
	awsDB = "aws://bucketdb-manifests:bucketdb-tables/p/test"
)

func commitValueToDB(t *testing.T, dbName string, value types.Value) {
	assert := assert.New(t)
	sp, err := spec.ForDataset(dbName + "::" + testDS)
	defer sp.Close()
	assert.NoError(err)
	_, err = sp.GetDatabase().CommitValue(sp.GetDataset(), value)
	assert.NoError(err)
}

func TestGetDatabase(t *testing.T) {
	assert := assert.New(t)
	testGet := func(dbName string) {
		commitValueToDB(t, dbName, testValue);
		for i := 0; i < 2; i++ {
			db, err := GetDatabase(dbName)
			assert.NoError(err)
			assert.NotNil(db)
			v := db.GetDataset(testDS).HeadValue()
			assert.Equal(testValue, v)
			// TODO: assert that they are sharing the same underlying chunkstore
		}
	}
	testGet(nbsDB)
	testGet(awsDB)
}

func TestPreload(t *testing.T) {
	assert := assert.New(t)
	testGet := func(dbName string) {
		commitValueToDB(t, dbName, testValue);
		err := Preload(dbName)
		assert.NoError(err)
		db, err := GetDatabase(dbName)
		assert.NoError(err)
		assert.NotNil(db)
		// TODO: need test to ensure 2nd read came from cache
		v := db.GetDataset(testDS).HeadValue()
		assert.Equal(testValue, v)
		// TODO: assert that they are sharing the same underlying chunkstore
	}
	testGet(nbsDB)
	testGet(awsDB)
}
