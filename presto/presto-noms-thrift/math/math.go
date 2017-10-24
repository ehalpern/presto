package math

import (
	"math"
)

func Round(v float64) float64 {
	return math.Floor(v + .5)

}

func DivInt64(v1, v2 int64) int64 {
	return int64(Round(float64(v1)/float64(v2)))
}

func DivUint64(v1, v2 uint64) uint64 {
	return uint64(Round(float64(v1)/float64(v2)))
}


func MinUint64(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func MaxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

