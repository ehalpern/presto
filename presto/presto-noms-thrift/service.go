package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type ServiceHandler struct {
	dbPrefix string
	schemas []string
}

func serviceError(format string, args... interface{}) *PrestoThriftServiceException {
	return &PrestoThriftServiceException{
		Message: fmt.Sprintf(format, args),
	}
}

// Returns available schema names.
func (h *ServiceHandler) PrestoListSchemaNames(ctx context.Context) (r []string, err error) {
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
		manifest := parts[1]
		query := &dynamodb.QueryInput{
			TableName: aws.String(manifest),
			KeyConditionExpression: aws.String("begins_with(db, :prefix)"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":prefix": {S: aws.String("/p/")},
			},
			ProjectionExpression: aws.String("db"), // is this required?
		}
		sess := session.Must(session.NewSession(aws.NewConfig().WithRegion("us-west-2")))
		svc := dynamodb.New(sess)
		result, err := svc.Query(query)
		if err != nil {
			return r, serviceError("failed to find manifests: %v", err)
		}
		if len(result.Items) != 1 {
			return r, serviceError("manifest query had no results")
		}
		for k, v := range result.Items[0] {
			if k == "db" {
				r = append(r, strings.TrimPrefix(*(v.S), "/p/"))
			}
		}
	}
	return r, nil
}

// Returns tables for the given schema name.
//
// @param schemaNameOrNull a structure containing schema name or {@literal null}
// @return a list of table names with corresponding schemas. If schema name is null then returns
// a list of tables for all schemas. Returns an empty list if a schema does not exist
//
// Parameters:
//  - SchemaNameOrNull
func (h *ServiceHandler) PrestoListTables(
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

func (h *ServiceHandler) dsSpec(schema, table string) (sp spec.Spec, err error) {
	if sp, err = spec.ForPath(h.dbPrefix + "/" + schema + "::" + table); err != nil {
		return sp, serviceError(err.Error())
	}
	return sp, nil
}

// Returns metadata for a given table.
//
// @param schemaTableName schema and table name
// @return metadata for a given table, or a {@literal null} value inside if it does not exist
//
// Parameters:
//  - SchemaTableName
func (h *ServiceHandler) PrestoGetTableMetadata(ctx context.Context, name *PrestoThriftSchemaTableName) (md *PrestoThriftNullableTableMetadata, err error) {
	table, err := getTable(h.dbPrefix, name)
	if err != nil {
		return md, err
	}
	defer table.Close()
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
//
// TODO: Use estimated size rather than rows to determine split
func (h *ServiceHandler) PrestoGetSplits(
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
	rowCount := table.getRowCount()
	splitCount := minUint64(rowCount / MinRowsPerSplit, uint64(maxSplitCount))
	rowsPerSplit := rowCount / splitCount
	var splits []*PrestoThriftSplit
	for i := uint64(0); i < splitCount; i++ {
		split := newSplit(tableName, i * rowsPerSplit, rowsPerSplit)
		splits = append(splits, &PrestoThriftSplit{SplitId: split.id()})
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
func (h *ServiceHandler) PrestoGetRows(ctx context.Context,
	splitId *PrestoThriftId, columns []string, maxBytes int64,
	nextToken *PrestoThriftNullableToken,
) (r *PrestoThriftPageResult_, err error) {
	batch := newBatch(splitId, nextToken)
	table, err := getTable(h.dbPrefix, batch.tableName())
	if err != nil {
		return r, err
	}
	defer table.Close()
	blocks, rowCount, err := table.getRows(batch, columns, maxBytes)
	if err != nil {
		return r, err
	}
	return &PrestoThriftPageResult_{
		ColumnBlocks: blocks,
		RowCount: rowCount,
		NextToken: batch.nextBatchToken(),
	}, nil
}

func minUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}
