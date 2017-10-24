package blocks

import (
	"fmt"
	"unsafe"

	. "prestothriftservice"

	"github.com/attic-labs/bucketdb/presto/presto-noms-thrift/math"
	"github.com/attic-labs/noms/go/d"
)

var (
	boolSize = int(unsafe.Sizeof(true))
	int32Size = int(unsafe.Sizeof(int32(1)))
	doubleSize = int(unsafe.Sizeof(float64(1)))
)

func AppendRows(blocks []*PrestoThriftBlock, more []*PrestoThriftBlock) []*PrestoThriftBlock {
	if len(blocks) == 0 {
		return more
	}
	d.PanicIfFalse(len(blocks) == len(more))
	for i, block := range blocks {
		appendToBlock(block, more[i])
	}
	return blocks
}

func RemoveRows(blocks []*PrestoThriftBlock, count uint64) []*PrestoThriftBlock {
	d.PanicIfFalse(len(blocks) > int(count))
	for _, block := range blocks {
		truncateBlock(block, count)
	}
	return blocks
}

func ByteCount(blocks []*PrestoThriftBlock) (count uint64) {
	if len(blocks) == 0 {
		return 0
	}
	for _, b := range blocks {
		count += blockByteCount(b)
	}
	return count
}

func RowCount(blocks []*PrestoThriftBlock) (count uint64) {
	if len(blocks) == 0 {
		return 0
	}
	return blockLen(blocks[0])
}

func BytesPerRow(blocks []*PrestoThriftBlock) (count uint64) {
	byteCount, rowCount := ByteCount(blocks), RowCount(blocks)
	if byteCount == 0 {
		return 0
	}
	return math.DivUint64(byteCount, rowCount)
}

func Validate(blocks []*PrestoThriftBlock) {
}

func AddToVarcharBlock(block *PrestoThriftBlock, bytes []byte, sizes []int32, nulls []bool) *PrestoThriftBlock {
	if block == nil {
		block = &PrestoThriftBlock{}
	}
	d.PanicIfFalse(len(sizes) == len(nulls))
	d := block.VarcharData
	if d == nil {
		block.VarcharData = NewPrestoThriftVarchar()
		d = block.VarcharData
	}
	d.Nulls = append(d.Nulls, nulls...)
	d.Sizes = append(d.Sizes, sizes...)
	d.Bytes = append(d.Bytes, bytes...)
	return block
}

func AddToDoubleBlock(block *PrestoThriftBlock, doubles []float64) *PrestoThriftBlock {
	if block == nil {
		block = &PrestoThriftBlock{}
	}
	d := block.DoubleData
	if d == nil {
		block.DoubleData = NewPrestoThriftDouble()
		d = block.DoubleData
	}
	d.Doubles = append(d.Doubles, doubles...)
	return block
}

func AddToBooleanBlock(block *PrestoThriftBlock, booleans []bool) *PrestoThriftBlock {
	if block == nil {
		block = &PrestoThriftBlock{}
	}
	d := block.BooleanData
	if d == nil {
		block.BooleanData = NewPrestoThriftBoolean()
		d = block.BooleanData
	}
	d.Booleans = append(d.Booleans, booleans...)
	return block
}

func EstimateRowByteCount(columns []string, md []*PrestoThriftColumnMetadata) (size uint64) {
	include := make(map[string]bool)
	for _, c := range columns {
		include[c] = true
	}
	for _, cm := range md {
		if include[cm.Name] {
			switch cm.Type {
			case "varchar":
				size += varcharByteCount("0123456789")
			case "boolean":
				size += booleanByteCount()
			case "double":
				size += doubleByteCount()
			default:
				panic(fmt.Errorf("unsupported row type %s", cm.Type))
			}
		}
	}
	return size
}

func doubleByteCount() uint64 {
	return uint64(doubleSize)
}

func booleanByteCount() uint64 {
	return uint64(boolSize)
}

func varcharByteCount(s string) uint64 {
	return uint64(int32Size) + // size array
	 		uint64(boolSize) +  // null array
			uint64(len([]byte(s))) // bytes array
}


func appendToBlock(block *PrestoThriftBlock, more *PrestoThriftBlock) *PrestoThriftBlock {
	if block.VarcharData != nil {
		d.PanicIfTrue(more.VarcharData == nil)
		AddToVarcharBlock(block, more.VarcharData.Bytes, more.VarcharData.Sizes, more.VarcharData.Nulls)
	} else if block.DoubleData != nil {
		d.PanicIfTrue(more.DoubleData == nil)
		AddToDoubleBlock(block, more.DoubleData.Doubles)
	} else if block.BooleanData != nil {
		d.PanicIfTrue(more.BooleanData == nil)
		AddToBooleanBlock(block, more.BooleanData.Booleans)
	} else {
		panic(fmt.Errorf("ThriftBlock data type not supprted: %s", block))
	}
	return block
}

func truncateBlock(block *PrestoThriftBlock, length uint64) *PrestoThriftBlock{
	if d := block.VarcharData; d != nil {
		d.Nulls = d.Nulls[:length]
		d.Sizes = d.Sizes[:length]
		byteCount := int32(0)
		for _, size := range d.Sizes {
			byteCount += size
		}
		d.Bytes = d.Bytes[:byteCount]
	} else if d := block.DoubleData; d != nil {
		d.Doubles = d.Doubles[:length]
	} else if d := block.BooleanData; d != nil {
		d.Booleans = d.Booleans[:length]
	} else {
		panic(fmt.Errorf("ThriftBlock data type not supprted: %s", block))
	}
	return block
}

func blockLen(block *PrestoThriftBlock) uint64 {
	if block.VarcharData != nil {
		return uint64(len(block.VarcharData.Sizes))
	}
	if block.DoubleData != nil {
		return uint64(len(block.DoubleData.Doubles))
	}
	if block.BooleanData != nil {
		return uint64(len(block.BooleanData.Booleans))
	}
	panic(fmt.Errorf("ThriftBlock data type not supprted: %s", block))
}

func blockByteCount(block *PrestoThriftBlock) (count uint64) {
	if block.VarcharData != nil {
		count += uint64(len(block.VarcharData.Sizes) * int32Size)
		count += uint64(len(block.VarcharData.Nulls) * boolSize)
		return count + uint64(len(block.VarcharData.Bytes))
	}
	if block.DoubleData != nil {
		return count + uint64(len(block.DoubleData.Doubles) * doubleSize)
	}
	if block.BooleanData != nil {
		return count + uint64(len(block.BooleanData.Booleans) * boolSize)
	}
	panic(fmt.Errorf("ThriftBlock data type not supported: %s", block))
}
