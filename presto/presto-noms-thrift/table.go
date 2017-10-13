package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"unsafe"

	. "prestothriftservice"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
)

type nomsTable interface {
	getMetadata() (*PrestoThriftTableMetadata, error)
	estimateRowSize(columns []string) uint64
	getRowCount() uint64
	getRows(batch *Batch, columns []string, maxBytes int64) (blocks []*PrestoThriftBlock, rowCount int32, err error)
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
	return estimateRowSize(columns, md.Columns)
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

// TODO: 1) parallelize across columns
//       2) use exact loading a la https://github.com/attic-labs/noms/issues/3619
func (t *colMajorTable) getRows(batch *Batch, columns []string, maxBytes int64) (blocks []*PrestoThriftBlock, rowCount int32, err error) {
	var limit uint64
	for _, c := range columns {
		v := t.s.Get(c)
		ref := v.(types.Ref)
		list := ref.TargetValue(t.sp.GetDatabase()).(types.List)
		offset := batch.Offset
		limit = minUint64(batch.Limit, list.Len())
		var block *PrestoThriftBlock
		elt := list.Get(0)
		switch elt.Kind() {
		case types.NumberKind:
			block = readDoubles(list, offset, limit)
		case types.BoolKind:
			block = readBools(list, offset, limit)
		case types.StringKind:
			block = readStrings(list, offset, limit)
		default:
			return blocks, rowCount, serviceError("unsupported column type %v", list.Kind())
		}
		blocks = append(blocks, block)
	}
	return blocks, int32(limit), nil
}


func readDoubles(list types.List, offset, limit uint64) *PrestoThriftBlock {
	numbers := make([]float64, limit)
	list.IterRange(offset, offset + limit -1, func(v types.Value, i uint64) {
		numbers[i - offset] = float64(v.(types.Number))
	})
	return &PrestoThriftBlock{
		DoubleData: &PrestoThriftDouble{
			Doubles: numbers[:],
		},
	}
}

func readBools(list types.List, offset, limit uint64) *PrestoThriftBlock {
	bools := make([]bool, limit)
	list.IterRange(offset, offset + limit -1, func(v types.Value, i uint64) {
		bools[i - offset] = bool(v.(types.Bool))
	})
	return &PrestoThriftBlock{
		BooleanData: &PrestoThriftBoolean{
			Booleans: bools[:],
		},
	}
}

func readStrings(list types.List, offset, limit uint64) *PrestoThriftBlock {
	nulls := make([]bool, limit)
	sizes := make([]int32, limit)
	var data bytes.Buffer
	list.IterRange(offset, offset + limit -1, func(v types.Value, i uint64) {
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
	return &PrestoThriftBlock{
		VarcharData: &PrestoThriftVarchar{
			Nulls: nulls[:],
			Sizes: sizes[:],
			Bytes: data.Bytes(),
		},
	}
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
	return estimateRowSize(columns, md.Columns)
}

func (t *rowMajorTable) getRowCount() uint64 {
	return 0
}

func (t *rowMajorTable) getRows(batch *Batch, columns []string, maxBytes int64,
) (blocks []*PrestoThriftBlock, rowCount int32, err error) {
	blocks = make([]*PrestoThriftBlock, len(columns))
	// assume list for now
	list := t.v.(types.List)
	limit := minUint64(batch.Limit, list.Len()-batch.Offset)
	it := list.IteratorAt(batch.Offset)
	for i, row := uint64(0), it.Next(); i < limit; i, row = i+1, it.Next() {
		st := row.(types.Struct)
		for j, col := range columns {
			switch v := st.Get(col).(type) {
			case types.Number:
				blocks[j] = appendDouble(blocks[j], float64(v))
			case types.Bool:
				blocks[j] = appendBool(blocks[j], bool(v))
			case types.String:
				blocks[j] = appendString(blocks[j], string(v))
			default:
				return blocks, rowCount, serviceError("unsupported column type %v", list.Kind())
			}
		}
	}
	return blocks, int32(limit), nil
}

func appendDouble(block *PrestoThriftBlock, d float64) *PrestoThriftBlock {
	if block == nil {
		block = &PrestoThriftBlock{DoubleData: NewPrestoThriftDouble()}
	}
	doubles := block.DoubleData.Doubles
	block.DoubleData.Doubles = append(doubles, d)
	return block
}

func appendBool(block *PrestoThriftBlock, b bool) *PrestoThriftBlock {
	if block == nil {
		block = &PrestoThriftBlock{BooleanData: NewPrestoThriftBoolean()}
	}
	bools := block.BooleanData.Booleans
	block.BooleanData.Booleans = append(bools, b)
	return block
}

func appendString(block *PrestoThriftBlock, s string) *PrestoThriftBlock {
	if block == nil {
		block = &PrestoThriftBlock{VarcharData: NewPrestoThriftVarchar()}
	}
	sizes := block.VarcharData.Sizes
	bytes := block.VarcharData.Bytes
	sBytes := []byte(s)
	block.VarcharData.Sizes = append(sizes, int32(len(sBytes)))
	block.VarcharData.Bytes = append(bytes, sBytes...)
	return block
}

var charSize = int(unsafe.Sizeof('a'))
var boolSize = int(unsafe.Sizeof(true))
var int32Size = int(unsafe.Sizeof(int32(1)))
var doubleSize = int(unsafe.Sizeof(float64(1)))

func estimateRowSize(columns []string, md []*PrestoThriftColumnMetadata) (size uint64) {
	include := make(map[string]bool)
	for _, c := range columns {
		include[c] = true
	}
	for _, cm := range md {
		if include[cm.Name] {
			switch cm.Type {
			case "varchar":
				size += uint64(int32Size)
				size += uint64(boolSize)
				size += uint64(charSize * 20)
			case "boolean":
				size += uint64(boolSize)
				size += uint64(boolSize)
			case "double":
				size += uint64(boolSize)
				size += uint64(doubleSize)
			default:
				log.Printf("unsupported row type %s", cm.Type)
			}
		}
	}
	return size
}

func blocksSize(blocks []*PrestoThriftBlock) (size uint64) {
	for _, b := range blocks {
		if b.VarcharData != nil {
			size += uint64(len(b.VarcharData.Sizes) * int32Size)
			size += uint64(len(b.VarcharData.Nulls) * boolSize)
			size += uint64(len(b.VarcharData.Bytes))
		}
		if b.DoubleData != nil {
			size += uint64(len(b.DoubleData.Nulls) * boolSize)
			size += uint64(len(b.DoubleData.Doubles) * doubleSize)
		}
		if b.BooleanData != nil {
			size += uint64(len(b.BooleanData.Nulls) * boolSize)
			size += uint64(len(b.BooleanData.Booleans) * boolSize)
		}
	}
	if len(blocks) == 0 {
		return 0
	}
	return size
}
