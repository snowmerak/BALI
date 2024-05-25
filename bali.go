package bali

import (
	"errors"
	"sync"
)

type Comparable[T any] interface {
	Compare(comparable2 Comparable[T]) int
}

type Node[T any] struct {
	Value    Comparable[T]
	RecordID uint64
	Next     *Node[T]
}

type IndexArray[T any] struct {
	Head  *Node[T]
	Count uint64
	Mutex sync.RWMutex
}

type Index[T any] struct {
	Array     []IndexArray[T]
	Count     uint64
	Threshold uint64
	Mutex     sync.RWMutex
}

func NewIndex[T any](threshold uint64) *Index[T] {
	return &Index[T]{
		Array:     make([]IndexArray[T], 0),
		Threshold: threshold,
	}
}

type ErrNotFound struct{}

func (e *ErrNotFound) Error() string {
	return "record not found"
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, &ErrNotFound{})
}

func (idx *Index[T]) binarySearchArray(value Comparable[T]) (int, error) {
	leftIndex := 0
	rightIndex := len(idx.Array) - 1

	for leftIndex <= rightIndex {
		midIndex := (leftIndex + rightIndex) / 2

		switch value.Compare(idx.Array[midIndex].Head.Value) {
		case -1:
			rightIndex = midIndex - 1
		case 0:
			return midIndex, nil
		case 1:
			if len(idx.Array)-1 == midIndex || (len(idx.Array)-1 > midIndex && value.Compare(idx.Array[midIndex+1].Head.Value) >= 0) {
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

func (idx *Index[T]) Insert(value Comparable[T], recordID uint64) error {
	idx.Mutex.Lock()
	defer idx.Mutex.Unlock()

	if len(idx.Array) == 0 {
		idx.Array = make([]IndexArray[T], 1)
		idx.Array[0].Head = &Node[T]{Value: value, RecordID: recordID}
		idx.Array[0].Count = 1
		idx.Count = 1
		return nil
	}

	leftIndex, _ := idx.binarySearchArray(value)

	idx.Array[leftIndex].Mutex.Lock()
	defer idx.Array[leftIndex].Mutex.Unlock()

	prev := (*Node[T])(nil)
	head := idx.Array[leftIndex].Head
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

			return nil
		case 0:
			if head.Next == nil {
				head.Next = &Node[T]{Value: value, RecordID: recordID}
				idx.Array[leftIndex].Count++
				idx.Count++
				return nil
			}

			if value.Compare(head.Next.Value) > 0 {
				newNode := &Node[T]{Value: value, RecordID: recordID, Next: head.Next}
				idx.Array[leftIndex].Count++
				idx.Count++
				head.Next = newNode
				return nil
			}
		case 1:
			if head.Next == nil {
				head.Next = &Node[T]{Value: value, RecordID: recordID}
				idx.Array[leftIndex].Count++
				idx.Count++
				return nil
			}
		}

		prev = head
		head = head.Next
	}

	// 균형 검사 및 재균형
	// ...

	return nil
}

// ... (재균형 함수 구현) ...
func (idx *Index[T]) rebalance() {
	// ... (재균형 로직 구현) ...
}
