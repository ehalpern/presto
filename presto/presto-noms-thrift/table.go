package main

import (
	"bytes"
	"fmt"
	"sync"

	. "prestothriftservice"

	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/blocks"
	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/math"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/nbs"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

var (
	ResultByteCountAccuracy = .9
)

type nomsTable interface {
	getMetadata() (*PrestoThriftTableMetadata, error)
	estimateRowSize(columns []string) uint64
	getRowCount() uint64
	getRows(columns []string, offset, limit, maxBytes uint64) (blocks []*PrestoThriftBlock, rowCount uint64, err error)
	stats() nbs.Stats
}

type colMajorTable struct {
	name *PrestoThriftSchemaTableName
	sp spec.Spec
	s types.Struct
}

type rowMajorTable struct {
	name *PrestoThriftSchemaTableName
	sp spec.Spec
	v types.Value
}

func dsSpec(prefix, schema, table string) (sp spec.Spec, err error) {
	if sp, err = spec.ForPath(prefix + "/" + schema + "::" + table); err != nil {
		return sp, serviceError(err.Error())
	}
	return sp, nil
}

type tableCacheT struct {
	cache map[PrestoThriftSchemaTableName]nomsTable
	lock sync.RWMutex
}

func (c *tableCacheT) get(name *PrestoThriftSchemaTableName) (t nomsTable, ok bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	t, ok = c.cache[*name]
	return
}

func (c *tableCacheT) put(name *PrestoThriftSchemaTableName, t nomsTable) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache[*name] = t
}

// TODO: need a method to close all cached table to exit tests cleanly

var tableCache = &tableCacheT{cache: make(map[PrestoThriftSchemaTableName]nomsTable)}

func getTable(dbPrefix string, name *PrestoThriftSchemaTableName) (t nomsTable, err error) {
	if t, ok := tableCache.get(name); ok {
		return t, nil
	}
	sp, err := dsSpec(dbPrefix, name.SchemaName, name.TableName)
	if err != nil {
		return nil, err
	}
	// TODO: validate entire type structure and return (nil, nil) if not valid
	switch table := sp.GetDataset().HeadValue().(type) {
	case types.List, types.Set, types.Map:
		t = &rowMajorTable{name, sp, table}
	case types.Struct:
		t = &colMajorTable{name, sp, table}
	default:
		return nil, nil
	}
	tableCache.put(name, t)
	return t, err
}

func (t *colMajorTable) getMetadata() (metadata *PrestoThriftTableMetadata, err error) {
	typeDesc := types.TypeOf(t.s).Desc.(types.StructDesc)
	var columns []*PrestoThriftColumnMetadata
	typeDesc.IterFields(func(name string, typ *types.Type, optional bool) {
		if typ.TargetKind() != types.RefKind {
			// ignore for now
			return
		}
		targetType := typ.Desc.(types.CompoundDesc).ElemTypes[0]
		if targetType.TargetKind() != types.ListKind {
			// ignore for now
			return
		}
		colType := targetType.Desc.(types.CompoundDesc).ElemTypes[0]
		columns = append(columns, &PrestoThriftColumnMetadata{
			Name: name,
			Type: kindToPrestoType[colType.TargetKind()],
		})
	})
	if len(columns) == 0 {
		return nil, serviceError("%v not a queriable type", t.name)
	}
	comment := "column-major"
	metadata = &PrestoThriftTableMetadata {
		t.name,
		columns,
		&comment,
	}
	return
}

var kindToPrestoType = map[types.NomsKind]string {
	types.BoolKind:   	"boolean",
	types.NumberKind: 	"double",
	types.StringKind: 	"varchar",
	// Convert the rest to string for now
	types.BlobKind:   	"varchar",
	types.ValueKind:  	"varchar",
	types.ListKind:		"varchar",
	types.MapKind:		"varchar",
	types.RefKind:		"varchar",
	types.SetKind:		"varchar",
	types.StructKind:	"varchar",
	types.CycleKind:	"varchar",
	types.TypeKind:		"varchar",
	types.UnionKind:	"varchar",
}

func (t *colMajorTable) estimateRowSize(columns []string) uint64 {
	md, err := t.getMetadata()
	d.PanicIfError(err)
	return blocks.EstimateRowByteCount(columns, md.Columns)
}


func (t *colMajorTable) getRowCount() uint64 {
	var firstColumn types.Value
	t.s.IterFields(func(_ string, v types.Value) {
		if firstColumn == nil {
			if r, ok := v.(types.Ref); ok {
				t := r.TargetValue(t.sp.GetDatabase())
				firstColumn, ok = t.(types.List)
				d.PanicIfFalse(ok)
			}
		}
	})
	if firstColumn == nil {
		return 0
	}
	return firstColumn.(types.List).Len()
}

func (t *colMajorTable) getRows(columns []string, offset, limit, maxBytes uint64) (blks []*PrestoThriftBlock, rowCount uint64, err error) {
	if len(columns) == 0 {
		// this is a row count query
		return blks, t.getRowCount(), nil
	}

	readColumns := func(cols []string, offset, limit uint64) (blks []*PrestoThriftBlock)  {
		var futureBlocks []<-chan *PrestoThriftBlock
		for _, c := range columns {
			v := t.s.Get(c)
			ref := v.(types.Ref)
			list := ref.TargetValue(t.sp.GetDatabase()).(types.List)
			limit = math.MinUint64(limit, list.Len() - offset)
			var block <-chan *PrestoThriftBlock
			elt := list.Get(0)
			switch elt.Kind() {
			case types.NumberKind:
				block = readDoubles(list, offset, limit)
			case types.BoolKind:
				block = readBools(list, offset, limit)
			case types.StringKind:
				block = readStrings(list, offset, limit)
			default:
				panic(fmt.Errorf("unexpected column type %v", list.Kind()))
			}
			futureBlocks = append(futureBlocks, block)
		}
		blks = make([]*PrestoThriftBlock, len(futureBlocks))
		for i, f := range futureBlocks {
			blks[i] = <- f
		}
		return blks
	}

	blks = readColumns(columns, offset, limit)
	rowCount = blocks.RowCount(blks)

	// If below target maxBytes, make one attempt to get closer
	if rowCount < t.getRowCount() - offset && float64(blocks.ByteCount(blks)) / float64(maxBytes) < ResultByteCountAccuracy {
		bytesToAdd := maxBytes - blocks.ByteCount(blks)
		rowsToAdd := math.DivUint64(bytesToAdd, blocks.BytesPerRow(blks))
		moreBlks := readColumns(columns, offset + blocks.RowCount(blks), rowsToAdd)
		blks = blocks.AppendRows(blks, moreBlks)
	}

	// If above target maxBytes, trim back
	if blocks.ByteCount(blks) > maxBytes {
		reduceBytes(blks, uint64(maxBytes))
	}

	return blks, blocks.RowCount(blks), nil
}

func  reduceBytes(blks []*PrestoThriftBlock, maxBytes uint64) {
	d.PanicIfFalse(blocks.ByteCount(blks) < uint64(maxBytes))
	bytesToRemove := blocks.ByteCount(blks) - maxBytes
	bytesPerRow := blocks.BytesPerRow(blks)
	rowsToRemove := math.DivUint64(bytesToRemove, bytesPerRow)
	blocks.RemoveRows(blks, rowsToRemove)
	for ; blocks.ByteCount(blks) > maxBytes; rowsToRemove <<= 1 {
		// Double rows removed on each iteration.
		blocks.RemoveRows(blks, rowsToRemove)
	}
}

func readDoubles(list types.List, offset, limit uint64) <-chan *PrestoThriftBlock {
	future := make(chan *PrestoThriftBlock, 1)
	go func() {
		defer close(future)
		numbers := make([]float64, limit)
		list.IterRange(offset, offset + limit -1, func(v types.Value, i uint64) {
			numbers[i - offset] = float64(v.(types.Number))
		})
		future <- blocks.AddToDoubleBlock(nil, numbers)
	}()
	return future
}

func readBools(list types.List, offset, limit uint64) <-chan *PrestoThriftBlock {
	future := make(chan *PrestoThriftBlock, 1)
	go func() {
		defer close(future)
		bools := make([]bool, limit)
		list.IterRange(offset, offset+limit-1, func(v types.Value, i uint64) {
			bools[i-offset] = bool(v.(types.Bool))
		})
		future <- blocks.AddToBooleanBlock(nil, bools)
	}()

	return future
}

func readStrings(list types.List, offset, limit uint64) <-chan *PrestoThriftBlock {
	future := make(chan *PrestoThriftBlock, 1)
	go func() {
		defer close(future)
		nulls := make([]bool, limit)
		sizes := make([]int32, limit)
		var data bytes.Buffer
		list.IterRange(offset, offset+limit-1, func(v types.Value, i uint64) {
			i = i - offset
			if v == nil {
				nulls[i] = true
				return
			}
			s := string(v.(types.String))
			if s == "" {
				nulls[i] = true
				return
			}
			n, err := data.WriteString(s)
			d.PanicIfError(err)
			nulls[i] = false
			sizes[i] = int32(n)
		})
		future <- blocks.AddToVarcharBlock(nil, data.Bytes(), sizes, nulls)
	}()
	return future
}

func (t *colMajorTable) stats() nbs.Stats {
	return t.sp.GetDatabase().Stats().(nbs.Stats)
}

func (t *rowMajorTable) getMetadata() (metadata *PrestoThriftTableMetadata, err error) {
	typ := types.TypeOf(t.v)
	var elementType *types.Type
	switch (typ.TargetKind()) {
	case types.ListKind, types.SetKind:
		elementType = typ.Desc.(types.CompoundDesc).ElemTypes[0];
	case types.MapKind:
		elementType = typ.Desc.(types.CompoundDesc).ElemTypes[1];
	default:
		panic("unreachable")
	}
	if elementType.TargetKind() != types.StructKind {
		return nil, fmt.Errorf("Row major dataset %v must be List|Set|Map<Struct>", typ.Describe())
	}
	var columns []*PrestoThriftColumnMetadata
	elementType.Desc.(types.StructDesc).IterFields(func(name string, t *types.Type, optional bool) {
		switch (t.TargetKind()) {
		case types.BoolKind, types.NumberKind, types.StringKind:
			columns = append(columns, &PrestoThriftColumnMetadata{
				Name: name,
				Type: kindToPrestoType[t.TargetKind()],
			})
		default:
			// ignore for now
			return
		}
	})
	if len(columns) == 0 {
		return nil, serviceError("%v not a queriable type", t.name)
	}
	comment := "row-major"
	metadata = &PrestoThriftTableMetadata {
			t.name,
			columns,
			&comment,
	}
	return
}

func (t *rowMajorTable) estimateRowSize(columns []string) uint64 {
	md, err := t.getMetadata()
	d.PanicIfError(err)
	return blocks.EstimateRowByteCount(columns, md.Columns)
}

func (t *rowMajorTable) getRowCount() uint64 {
	return t.v.(types.List).Len()
}

func (t *rowMajorTable) getRows(columns []string, offset, limit, maxBytes uint64,
) (rows []*PrestoThriftBlock, rowCount uint64, err error) {
	if len(columns) == 0 {
		// this is a row count query
		return rows, t.getRowCount(), nil
	}
	rows = make([]*PrestoThriftBlock, len(columns))
	// assume list for now
	list := t.v.(types.List)
	limit = math.MinUint64(limit, list.Len()-offset)
	it := list.IteratorAt(offset)
	for i, row := uint64(0), it.Next(); i < limit; i, row = i+1, it.Next() {
		st := row.(types.Struct)
		for j, col := range columns {
			switch v := st.Get(col).(type) {
			case types.Number:
				blocks.AddToDoubleBlock(rows[j], []float64{float64(v)})
			case types.Bool:
				blocks.AddToBooleanBlock(rows[j], []bool{bool(v)})
			case types.String:
				s := string(v)
				blocks.AddToVarcharBlock(rows[j], []byte(s), []int32{int32(len(s))}, []bool{false})
			default:
				return rows, rowCount, serviceError("unsupported column type %v", list.Kind())
			}
		}
	}
	return rows, limit, nil
}

func (t *rowMajorTable) stats() nbs.Stats {
	return t.sp.GetDatabase().Stats().(nbs.Stats)
}
