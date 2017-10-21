package main

import (
	"math"
)

func round(v float64) float64 {
	return math.Floor(v + .5)

}
func divUint64(v1, v2 uint64) uint64 {
	return uint64(round(float64(v1)/float64(v2)))
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

