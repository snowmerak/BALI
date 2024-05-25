package bali

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type Node[T any] struct {
	Value    Comparable[T]
	RecordID uint64
	Next     *Node[T]
}

func (n *Node[T]) GoString() string {
	return fmt.Sprintf("{Value: %v, RecordID: %v}", n.Value, n.RecordID)
}

type IndexArray[T any] struct {
	Head  *Node[T]
	Count uint64
	Mutex sync.RWMutex
}

func (ia *IndexArray[T]) GoString() string {
	builder := strings.Builder{}
	for head := ia.Head; head != nil; head = head.Next {
		builder.WriteString(head.GoString())

		if head.Next != nil {
			builder.WriteString("->")
		}
	}

	return builder.String()
}

type Index[T any] struct {
	Array     []*IndexArray[T]
	Count     uint64
	Threshold uint64
	Mutex     sync.RWMutex
}

func NewIndex[T any](threshold uint64) *Index[T] {
	return &Index[T]{
		Array:     make([]*IndexArray[T], 0),
		Threshold: threshold,
	}
}

type ErrEmptyIndex struct{}

func (e *ErrEmptyIndex) Error() string {
	return "index is empty"
}

func IsEmptyIndexErr(err error) bool {
	return errors.Is(err, &ErrEmptyIndex{})
}

type ErrTooSmall struct{}

func (e *ErrTooSmall) Error() string {
	return "value is too small"
}

func IsTooSmallErr(err error) bool {
	return errors.Is(err, &ErrTooSmall{})
}

type ErrNotFound struct{}

func (e *ErrNotFound) Error() string {
	return "record not found"
}

func IsNotFoundErr(err error) bool {
	return errors.Is(err, &ErrNotFound{})
}

func (idx *Index[T]) binarySearchArray(value Comparable[T]) (int, error) {
	if len(idx.Array) == 0 {
		return -1, new(ErrEmptyIndex)
	}

	leftIndex := 0
	rightIndex := len(idx.Array) - 1

	if value.Compare(idx.Array[0].Head.Value) < 0 {
		return -1, new(ErrTooSmall)
	}

	for leftIndex <= rightIndex {
		midIndex := (leftIndex + rightIndex) / 2

		switch value.Compare(idx.Array[midIndex].Head.Value) {
		case -1:
			rightIndex = midIndex - 1
		case 0:
			return midIndex, nil
		case 1:
			if len(idx.Array)-1 == midIndex {
				return midIndex, nil
			}

			if value.Compare(idx.Array[midIndex+1].Head.Value) < 0 {
				return midIndex, nil
			}

			leftIndex = midIndex + 1
		}
	}

	return leftIndex, new(ErrNotFound)
}

func (idx *Index[T]) Search(value Comparable[T]) (uint64, error) {
	idx.Mutex.RLock()
	defer idx.Mutex.RUnlock()

	leftIndex, err := idx.binarySearchArray(value)
	if err != nil {
		return 0, err
	}

	if leftIndex == -1 {
		leftIndex = 0
	}

	idx.Array[leftIndex].Mutex.RLock()
	defer idx.Array[leftIndex].Mutex.RUnlock()

	if leftIndex == len(idx.Array) {
		return 0, new(ErrNotFound)
	}

	head := idx.Array[leftIndex].Head
	for head != nil {
		if head.Value.Compare(value) == 0 {
			return head.RecordID, nil
		}

		head = head.Next
	}

	return 0, new(ErrNotFound)
}

func (idx *Index[T]) SearchRange(start Comparable[T], end Comparable[T], callback func(uint64) error) error {
	idx.Mutex.RLock()
	defer idx.Mutex.RUnlock()

	startArrayIndex, err := idx.binarySearchArray(start)
	if err != nil {
		return fmt.Errorf("cannot find index of start: %w", err)
	}

	endArrayIndex, err := idx.binarySearchArray(end)
	if err != nil {
		return fmt.Errorf("cannot find index of end: %w", err)
	}

	for index := startArrayIndex; index <= endArrayIndex; index++ {
		arr := idx.Array[index]

		switch index {
		case startArrayIndex:
			head := arr.Head
			for head != nil {
				if start.Compare(head.Value) <= 0 {
					if err := callback(head.RecordID); err != nil {
						return fmt.Errorf("callback error: %w", err)
					}
				}

				head = head.Next
			}
		case endArrayIndex:
			head := arr.Head
			for head != nil {
				if end.Compare(head.Value) >= 0 {
					if err := callback(head.RecordID); err != nil {
						return fmt.Errorf("callback error: %w", err)
					}
				} else {
					break
				}

				head = head.Next
			}
		default:
			head := arr.Head
			for head != nil {
				if err := callback(head.RecordID); err != nil {
					return fmt.Errorf("callback error: %w", err)
				}

				head = head.Next
			}
		}
	}

	return nil
}

func (idx *Index[T]) Insert(value Comparable[T], recordID uint64) error {
	idx.Mutex.Lock()
	defer idx.Mutex.Unlock()

	if len(idx.Array) == 0 {
		idx.Array = make([]*IndexArray[T], 1)
		idx.Array[0] = &IndexArray[T]{Head: &Node[T]{Value: value, RecordID: recordID}, Count: 1}
		idx.Count = 1
		return nil
	}

	leftIndex, _ := idx.binarySearchArray(value)
	if leftIndex < 0 {
		leftIndex = 0
	}

	idx.Array[leftIndex].Mutex.Lock()
	defer idx.Array[leftIndex].Mutex.Unlock()

	prev := (*Node[T])(nil)
	head := idx.Array[leftIndex].Head
loop:
	for head != nil {
		switch value.Compare(head.Value) {
		case -1:
			newNode := &Node[T]{Value: value, RecordID: recordID, Next: head}
			idx.Array[leftIndex].Count++
			idx.Count++
			head = newNode

			if prev != nil {
				prev.Next = newNode
			}

			if idx.Array[leftIndex].Head.RecordID == head.Next.RecordID {
				idx.Array[leftIndex].Head = head
			}

			break loop
		case 0:
			if head.Next == nil {
				head.Next = &Node[T]{Value: value, RecordID: recordID}
				idx.Array[leftIndex].Count++
				idx.Count++
				break loop
			}

			if value.Compare(head.Next.Value) > 0 {
				newNode := &Node[T]{Value: value, RecordID: recordID, Next: head.Next}
				idx.Array[leftIndex].Count++
				idx.Count++
				head.Next = newNode
				break loop
			}
		case 1:
			if head.Next == nil {
				head.Next = &Node[T]{Value: value, RecordID: recordID}
				idx.Array[leftIndex].Count++
				idx.Count++
				break loop
			}

			if value.Compare(head.Next.Value) <= 0 {
				newNode := &Node[T]{Value: value, RecordID: recordID, Next: head.Next}
				idx.Array[leftIndex].Count++
				idx.Count++
				head.Next = newNode
				break loop
			}
		}

		prev = head
		head = head.Next
	}

	if idx.Array[leftIndex].Count < idx.Threshold {
		return nil
	}

	count := uint64(0)
	prev = nil
	head = idx.Array[leftIndex].Head
	for head != nil {
		prev = head
		head = head.Next
		count++

		if count == idx.Array[leftIndex].Count/2 {
			break
		}
	}

	prev.Next = nil
	totalCount := idx.Array[leftIndex].Count
	idx.Array[leftIndex].Count = count

	newArray := make([]*IndexArray[T], len(idx.Array)+1)
	copy(newArray, idx.Array[:leftIndex+1])
	newArray[leftIndex+1] = &IndexArray[T]{Head: head, Count: totalCount - count}
	copy(newArray[leftIndex+2:], idx.Array[leftIndex+1:])
	idx.Array = newArray

	return nil
}

func (idx *Index[T]) Delete(value Comparable[T], recordID uint64) bool {
	idx.Mutex.Lock()
	defer idx.Mutex.Unlock()

	if len(idx.Array) == 0 {
		return false
	}

	leftIndex, err := idx.binarySearchArray(value)
	if err != nil {
		return false
	}

	if leftIndex < 0 {
		leftIndex = 0
	}

	idx.Array[leftIndex].Mutex.Lock()
	defer idx.Array[leftIndex].Mutex.Unlock()

	prev := (*Node[T])(nil)
	head := idx.Array[leftIndex].Head
	for head != nil {
		switch value.Compare(head.Value) {
		case -1:
		case 0:
			if recordID == head.RecordID {
				if prev != nil {
					prev.Next = head.Next
				}

				if idx.Array[leftIndex].Head.RecordID == head.RecordID {
					idx.Array[leftIndex].Head = head.Next
				}

				if idx.Array[leftIndex].Head == nil {
					idx.Array = append(idx.Array[:leftIndex], idx.Array[leftIndex+1:]...)
				}
			}
		case 1:
			return false
		}

		prev = head
		head = head.Next
	}

	return false
}

func (idx *Index[T]) GoString() string {
	idx.Mutex.RLock()
	defer idx.Mutex.RUnlock()

	builder := strings.Builder{}
	for i := range idx.Array {
		if i > 0 {
			builder.WriteString("\n")
		}

		realLength := 0
		head := idx.Array[i].Head
		for head != nil {
			realLength++
			head = head.Next
		}

		builder.WriteString(" List[")
		builder.WriteString(strconv.FormatUint(idx.Array[i].Count, 10))
		builder.WriteString("]: ")
		builder.WriteString(idx.Array[i].GoString())
	}

	return builder.String()
}
