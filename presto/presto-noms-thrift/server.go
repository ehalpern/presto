package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	. "prestothriftservice"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Starts thrift server
func runServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, dbPrefix string) error {
	var transport thrift.TServerTransport
	var err error
	transport, err = thrift.NewTServerSocket(addr)

	if err != nil {
		return err
	}
	handler := &thriftHandler{
		dbPrefix: dbPrefix,
	}
	processor := NewPrestoThriftServiceProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	log.Printf("Starting thrift server... on %v (%T)", addr, transport)
	return server.Serve()
}

type thriftHandler struct {
	dbPrefix string
	schemas []string
}

func serviceError(format string, args... interface{}) *PrestoThriftServiceException {
	return &PrestoThriftServiceException{
		Message: fmt.Sprintf(format, args),
	}
}

// Returns available schema names by list the noms databases under dbPrefix
func (h *thriftHandler) PrestoListSchemaNames(ctx context.Context) (r []string, err error) {
	parts := strings.Split(h.dbPrefix, ":")
	protocol:= parts[0]
	switch protocol {
	case "nbs":
		path := parts[1]
		dirs, err := ioutil.ReadDir(path)
		if err != nil {
			return r, serviceError("dbPrefix path %s is not a directory", path)
		}
		for _, d := range dirs {
			if d.IsDir() {
				r = append(r, d.Name())
			}
		}
	case "aws":
		manifest := strings.Trim(parts[1], "//")
		scan := &dynamodb.ScanInput{
			TableName: aws.String(manifest),
			FilterExpression: aws.String("begins_with(db, :prefix)"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("/p/")},
			},
			ProjectionExpression: aws.String("db"),
		}
		sess := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
		svc := dynamodb.New(sess)
		for ;; {
			result, err := svc.Scan(scan)
			if err != nil {
				return r, serviceError("failed to find manifests: %v", err)
			}
			for _, v := range result.Items {
				if att, ok := v["db"]; ok {
					r = append(r, strings.TrimPrefix(*att.S, "/p/"))
				}
			}
			if result.LastEvaluatedKey == nil {
				break
			}
			scan.ExclusiveStartKey = result.LastEvaluatedKey
		}
	}
	return r, nil
}

// Returns tables for the given schema name
//
// Iterates the datasets in the database named by |schema|
//
// @param schemaNameOrNull a structure containing schema name or {@literal null}
// @return a list of table names with corresponding schemas. If schema name is null then returns
// a list of tables for all schemas. Returns an empty list if a schema does not exist
//
// Parameters:
//  - SchemaNameOrNull
func (h *thriftHandler) PrestoListTables(
	ctx context.Context, schema *PrestoThriftNullableSchemaName,
) (tables []*PrestoThriftSchemaTableName, err error) {
	// list tables in schema
	listTables := func(s string) (tables []*PrestoThriftSchemaTableName, err error) {
		spec, err := spec.ForDatabase(h.dbPrefix + "/" + s)
		if err != nil {
			return tables, serviceError(err.Error())
		}
		defer spec.Close()
		d.PanicIfError(err)
		db := spec.GetDatabase()
		db.Datasets().IterAll(func(name, _ types.Value) {
			tables = append(tables, &PrestoThriftSchemaTableName{
				s, string(name.(types.String)),
			})
		})
		return tables, nil
	}
	if schema.IsSetSchemaName() {
		return listTables(schema.GetSchemaName())
	}
	schemas, err := h.PrestoListSchemaNames(ctx)
	if err != nil {
		return tables, err
	}
	for _, schema := range schemas {
		t, err := listTables(schema)
		if err != nil {
			return tables, err
		}
		tables = append(tables, t...)
	}
	return tables, nil
}

func (h *thriftHandler) dsSpec(schema, table string) (sp spec.Spec, err error) {
	if sp, err = spec.ForPath(h.dbPrefix + "/" + schema + "::" + table); err != nil {
		return sp, serviceError(err.Error())
	}
	return sp, nil
}

// Returns metadata for a given table.
//
// Maps the column or row major structure to presto column structure
//
// @param schemaTableName schema and table name
// @return metadata for a given table, or a {@literal null} value inside if it does not exist
func (h *thriftHandler) PrestoGetTableMetadata(ctx context.Context, name *PrestoThriftSchemaTableName) (md *PrestoThriftNullableTableMetadata, err error) {
	table, err := getTable(h.dbPrefix, name)
	if err != nil {
		return md, err
	}
	metadata, err := table.getMetadata()
	if err != nil {
		return md, err
	}
	return &PrestoThriftNullableTableMetadata{metadata}, nil
}


// Returns a batch of splits.
//
// @param schemaTableName schema and table name
// @param desiredColumns a superset of columns to return; empty set means "no columns", {@literal null} set means "all columns"
// @param outputConstraint constraint on the returned data
// @param maxSplitCount maximum number of splits to return
// @param nextToken token from a previous split batch or {@literal null} if it is the first call
// @return a batch of splits
//
// Parameters:
//  - SchemaTableName
//  - DesiredColumns
//  - OutputConstraint
//  - MaxSplitCount
//  - NextToken
func (h *thriftHandler) PrestoGetSplits(
	ctx context.Context,
	tableName *PrestoThriftSchemaTableName,
	desiredColumns *PrestoThriftNullableColumnSet,
	outputConstraint *PrestoThriftTupleDomain,
	maxSplitCount int32,
	nextToken *PrestoThriftNullableToken,
) (splitBatch *PrestoThriftSplitBatch, err error) {
	table, err := getTable(h.dbPrefix, tableName)
	if err != nil {
		return
	}
	var splits []*PrestoThriftSplit
	rowCount := table.getRowCount()
	if len(desiredColumns.Columns) == 0 {
		countSplit := &PrestoThriftSplit{
			SplitId: newSplit(tableName, uint64(0), uint64(rowCount), uint64(0)).id(),
		}
		splits = append(splits, countSplit)
		log.Printf("Using single split to count rows")
	} else {
		estBytesPerRow := table.estimateRowSize(desiredColumns.Columns)
		minRowsPerSplit := config.minBytesPerSplit / estBytesPerRow
		maxSplits := config.nodeCount
		splitCount := maxUint64(1, minUint64(rowCount/minRowsPerSplit, maxSplits))
		rowsPerSplit := rowCount / splitCount
		remainder := rowCount % rowsPerSplit
		for i := uint64(0); i < splitCount; i++ {
			limit := rowsPerSplit
			if i+1 == splitCount && remainder > 0 {
				limit = remainder
			}
			split := newSplit(tableName, i*rowsPerSplit, limit, estBytesPerRow)
			// TODO: Specify Host to control which node each split is run on. The
			// thrift connector doesn't have knowledge of presto nodes, so we
			// need an independent mechanism for discovering them. In AWS
			// this can be accomplished by querying the autoscaler.
			splits = append(splits, &PrestoThriftSplit{SplitId: split.id()})
		}
		log.Printf("Splitting query into %d splits of %d rows of %d estimated bytes", splitCount, rowsPerSplit, estBytesPerRow)
	}
	return &PrestoThriftSplitBatch{
		Splits: splits,
		NextToken: nil,
	}, nil
}

// Returns a batch of rows for the given split.
//
// @param splitId split id as returned in split batch
// @param columns a list of column names to return
// @param maxBytes maximum size of returned data in bytes
// @param nextToken token from a previous batch or {@literal null} if it is the first call
// @return a batch of table data
//
// Parameters:
//  - SplitId
//  - Columns
//  - MaxBytes
//  - NextToken
func (h *thriftHandler) PrestoGetRows(ctx context.Context,
	splitId *PrestoThriftId, columns []string, maxBytes int64,
	nextToken *PrestoThriftNullableToken,
) (r *PrestoThriftPageResult_, err error) {
	start := time.Now()
	batch := newBatch(splitId, nextToken.Token, maxBytes)
	table, err := getTable(h.dbPrefix, batch.tableName())
	stats := table.stats()
	if err != nil {
		return r, err
	}
	log.Printf("Reading\t%d rows starting from %d", batch.Limit, batch.Offset)
	blocks, rowCount, err := table.getRows(batch, columns, maxBytes)
	if err != nil {
		return r, err
	}

	bytesRetrieved := blocksSize(blocks)
	elapsed := time.Now().Sub(start)
	delta := table.stats().Delta(stats)
	log.Printf("Read\t%d rows (%d bytes) in %d ms (%.f%% of %d max bytes)", rowCount, bytesRetrieved, elapsed.Nanoseconds() / 1e6, float64(bytesRetrieved)/float64(maxBytes) * 100, maxBytes)
	log.Printf(`---NBS Stats---
GetLatency:                       %s
ChunksPerGet:                     %s
S3ReadLatency:                    %s
S3BytesPerRead:                   %s
`,
		delta.GetLatency,
		delta.ChunksPerGet,
		delta.S3ReadLatency,
		delta.S3BytesPerRead)

	return &PrestoThriftPageResult_{
		ColumnBlocks: blocks,
		RowCount: int32(rowCount),
		NextToken: batch.nextBatchId(maxBytes, bytesRetrieved/batch.Limit),
	}, nil
}

func minUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func maxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}
