package blocks

import (
	. "prestothriftservice"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVarcharBlock(t *testing.T) {
	assert := assert.New(t)
	verifyBlock := func(b *PrestoThriftBlock, content []string) {
		expectedLen := uint64(len(content))
		expectedBytes, expectedSizes, expectedByteCount := func() (bytes []byte, sizes []int32, byteCount uint64) {
			for _, s := range content {
				byteCount += varcharByteCount(s)
				sizes = append(sizes, int32(len(s)))
				bytes = append(bytes, []byte(s)...)

			}
			return
		}()
		assert.Equal(expectedLen, blockLen(b))
		assert.Equal(expectedByteCount, blockByteCount(b))
		assert.Equal(expectedSizes, b.VarcharData.Sizes)
		assert.Equal(expectedBytes, b.VarcharData.Bytes)
	}
	var b *PrestoThriftBlock
	content := []string{ "row0", "row1", "row2", "row3", "row4" }
	for _, s := range content {
		b = AddToVarcharBlock(b, []byte(s), []int32{int32(len(s))}, []bool{false})
	}
	verifyBlock(b, content)

	b = AddToVarcharBlock(b, []byte("r5row6"), []int32{2, 4}, []bool{false,false})
	content = append(content, "r5", "row6")
	verifyBlock(b, content)

	content = content[:5]
	b = truncateBlock(b, uint64(len(content)))
	verifyBlock(b, content)
}

func TestDoubleBlock(t *testing.T) {
	assert := assert.New(t)
	verifyBlock := func(b *PrestoThriftBlock, content []float64) {
		expectedLen := uint64(len(content))
		expectedByteCount := expectedLen * doubleByteCount()
		expectedValues := content
		assert.Equal(expectedLen, blockLen(b))
		assert.Equal(expectedByteCount, blockByteCount(b))
		assert.Equal(expectedValues, b.DoubleData.Doubles)
	}
	var b *PrestoThriftBlock
	content := []float64{ 1.0, 2.1, 3.2, 4.3, 5.4 }
	for _, d := range content {
		b = AddToDoubleBlock(b, []float64{d})
	}
	verifyBlock(b, content)

	moreContent := []float64{6.5, 7.6}
	b = AddToDoubleBlock(b, moreContent)
	content = append(content, moreContent...)
	verifyBlock(b, content)

	content = content[:5]
	b = truncateBlock(b, uint64(len(content)))
	verifyBlock(b, content)
}

func TestBooleanBlock(t *testing.T) {
	assert := assert.New(t)
	verifyBlock := func(b *PrestoThriftBlock, content []bool) {
		expectedLen := uint64(len(content))
		expectedByteCount := expectedLen * booleanByteCount()
		expectedValues := content
		assert.Equal(expectedLen, blockLen(b))
		assert.Equal(expectedByteCount, blockByteCount(b))
		assert.Equal(expectedValues, b.BooleanData.Booleans)
	}
	var b *PrestoThriftBlock
	content := []bool{ true, false, false, true, true }
	for _, d := range content {
		b = AddToBooleanBlock(b, []bool{d})
	}
	verifyBlock(b, content)

	moreContent := []bool{false, true}
	b = AddToBooleanBlock(b, moreContent)
	content = append(content, moreContent...)
	verifyBlock(b, content)

	content = content[:5]
	b = truncateBlock(b, uint64(len(content)))
	verifyBlock(b, content)
}


func TestRows(t *testing.T) {
	assert := assert.New(t)
	verifyBlocks := func(blocks, expected []*PrestoThriftBlock) {
		for i, b := range blocks {
			assert.Equal(expected[i].String(), b.String())
		}

	}

	threeRows := []*PrestoThriftBlock{
		{
			VarcharData: &PrestoThriftVarchar{
				Nulls: []bool{false, false, false},
				Sizes: []int32{4, 4, 4},
				Bytes: []byte("row0row1row2"),
			},
		},
		{
			DoubleData: &PrestoThriftDouble{
				Doubles: []float64{1.0, 2.1, 3.2},
			},
		}, {
			BooleanData: &PrestoThriftBoolean{
				Booleans: []bool{true, true, false},
			},
		},
	}

	blocks := AppendRows([]*PrestoThriftBlock{}, threeRows)
	verifyBlocks(blocks, threeRows)

	twoMore := []*PrestoThriftBlock{
		{
			VarcharData: &PrestoThriftVarchar{
				Nulls: []bool{false, false},
				Sizes: []int32{2, 4},
				Bytes: []byte("r3row4"),
			},
		},
		{
			DoubleData: &PrestoThriftDouble{
				Doubles: []float64{4.3, 5.4},
			},
		},
		{
			BooleanData: &PrestoThriftBoolean{
				Booleans: []bool{false, true},
			},
		},
	}

	fiveRows := []*PrestoThriftBlock{
		{
			VarcharData: &PrestoThriftVarchar{
				Nulls: []bool{false, false, false, false, false},
				Sizes: []int32{4, 4, 4, 2, 4},
				Bytes: []byte("row0row1row2r3row4"),
			},
		},
		{
			DoubleData: &PrestoThriftDouble{
				Doubles: []float64{1.0, 2.1, 3.2, 4.3, 5.4},
			},
		}, {
			BooleanData: &PrestoThriftBoolean{
				Booleans: []bool{true, true, false, false, true},
			},
		},
	}

	blocks = AppendRows(blocks, twoMore)
	verifyBlocks(blocks, fiveRows)

	blocks = RemoveRows(blocks, 2)
	verifyBlocks(blocks, threeRows)
}
