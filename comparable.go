package bali

import "time"

type Comparable[T any] interface {
	Compare(other Comparable[T]) int
}

type U8 uint8

func (i U8) Compare(other Comparable[U8]) int {
	l := uint8(i)
	r := uint8(other.(U8))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type U16 uint16

func (i U16) Compare(other Comparable[U16]) int {
	l := uint16(i)
	r := uint16(other.(U16))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type U32 uint32

func (i U32) Compare(other Comparable[U32]) int {
	l := uint32(i)
	r := uint32(other.(U32))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type U64 uint64

func (i U64) Compare(other Comparable[U64]) int {
	l := uint64(i)
	r := uint64(other.(U64))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type Uint uint64

func (i Uint) Compare(other Comparable[Uint]) int {
	l := uint64(i)
	r := uint64(other.(Uint))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type I8 int8

func (i I8) Compare(other Comparable[I8]) int {
	l := int8(i)
	r := int8(other.(I8))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type I16 int16

func (i I16) Compare(other Comparable[I16]) int {
	l := int16(i)
	r := int16(other.(I16))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type I32 int32

func (i I32) Compare(other Comparable[I32]) int {
	l := int32(i)
	r := int32(other.(I32))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type I64 int64

func (i I64) Compare(other Comparable[I64]) int {
	l := int64(i)
	r := int64(other.(I64))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type Int int64

func (i Int) Compare(other Comparable[Int]) int {
	l := int64(i)
	r := int64(other.(Int))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type Float32 float32

func (i Float32) Compare(other Comparable[Float32]) int {
	l := float32(i)
	r := float32(other.(Float32))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type Float64 float64

func (i Float64) Compare(other Comparable[Float64]) int {
	l := float64(i)
	r := float64(other.(Float64))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}

type Time time.Time

func (i Time) Compare(other Comparable[Time]) int {
	l := time.Time(i)
	r := time.Time(other.(Time))
	if l.Before(r) {
		return -1
	} else if l.After(r) {
		return 1
	} else {
		return 0
	}
}

type String string

func (i String) Compare(other Comparable[String]) int {
	l := string(i)
	r := string(other.(String))
	if l < r {
		return -1
	} else if l > r {
		return 1
	} else {
		return 0
	}
}
