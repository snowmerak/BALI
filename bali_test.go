package bali_test

import (
	bali "github.com/snowmerak/BALI"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const threshold = 128
const length = 1000000

func TestInsertAndSearch(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	for i := uint64(0); i < length; i++ {
		err := idx.Insert(bali.U64(i), i)
		if err != nil {
			t.Errorf("Error inserting value %d: %v", i, err)
		}
	}

	for i := uint64(0); i < length; i++ {
		recordID, err := idx.Search(bali.U64(i))
		if err != nil {
			t.Errorf("Error searching for value %d: %v", i, err)
		}

		if recordID != i {
			t.Errorf("Expected record ID %d, got %d", i, recordID)
		}
	}
}

func TestRandomInsertAndSearch(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	indices := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	for _, i := range indices {
		err := idx.Insert(bali.U64(i), i)
		if err != nil {
			t.Errorf("Error inserting value %d: %v", i, err)
		}
	}

	for i := uint64(0); i < length; i++ {
		recordID, err := idx.Search(bali.U64(i))
		if err != nil {
			t.Errorf("Error searching for value %d: %v", i, err)
		}

		if recordID != i {
			t.Errorf("Expected record ID %d, got %d", i, recordID)
		}
	}
}

func TestSearchRange(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	indices := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	for _, i := range indices {
		err := idx.Insert(bali.U64(i), i)
		if err != nil {
			t.Errorf("Error inserting value %d: %v", i, err)
		}
	}

	start := uint64(150)
	end := uint64(9750)

	foundIDs := make([]uint64, 0, end-start+1)
	err := idx.SearchRange(bali.U64(start), bali.U64(end), func(recordID uint64) error {
		foundIDs = append(foundIDs, recordID)
		return nil
	})
	if err != nil {
		t.Errorf("Error searching for range: %v", err)
	}

	if uint64(len(foundIDs)) != end-start+1 {
		t.Errorf("Expected %d IDs, got %d", end-start+1, len(foundIDs))
	}

	for i := start; i <= end; i++ {
		found := false
		for _, id := range foundIDs {
			if id == i {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("ID %d not found in search range", i)
		}
	}
}

func TestDelete(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	indices := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	for _, i := range indices {
		err := idx.Insert(bali.U64(i), i)
		if err != nil {
			t.Errorf("Error inserting value %d: %v", i, err)
		}
	}

	indices = make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	for _, i := range indices {
		deleted := idx.Delete(bali.U64(i), i)
		if !deleted {
			t.Errorf("Failed to delete value %d", i)
			t.Log(idx.GoString())
		}
	}

	for i := uint64(0); i < length; i++ {
		_, err := idx.Search(bali.U64(i))
		if err == nil {
			t.Errorf("Value %d found after deletion", i)
		}
	}
}

func TestEmptyIndex(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	_, err := idx.Search(bali.U64(100))
	if err == nil {
		t.Errorf("Expected error searching in empty index")
	}

	if !bali.IsEmptyIndexErr(err) {
		t.Errorf("Expected EmptyIndexErr, got %v", err)
	}
}

func TestTooSmallValue(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	err := idx.Insert(bali.U64(100), 100)
	if err != nil {
		t.Errorf("Error inserting value 100: %v", err)
	}

	_, err = idx.Search(bali.U64(50))
	if err == nil {
		t.Errorf("Expected error searching for value 50")
	}

	if !bali.IsTooSmallErr(err) {
		t.Errorf("Expected TooSmallErr, got %v", err)
	}
}

func TestNotFound(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	if err := idx.Insert(bali.U64(50), 50); err != nil {
		t.Errorf("Error inserting value 50: %v", err)
	}

	_, err := idx.Search(bali.U64(100))
	if err == nil {
		t.Errorf("Expected error searching for value 100")
	}

	if !bali.IsNotFoundErr(err) {
		t.Errorf("Expected NotFoundErr, got %v", err)
	}
}

func TestRaceCondition(t *testing.T) {
	idx := bali.NewIndex[bali.U64](threshold)

	indices := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	wg := new(sync.WaitGroup)

	for _, i := range indices {
		if err := idx.Insert(bali.U64(i), i); err != nil {
			t.Errorf("Error inserting value %d: %v", i, err)
		}

		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()

			<-time.After(1 * time.Millisecond)
			if _, err := idx.Search(bali.U64(i)); err != nil {
				t.Errorf("Error searching for value %d: %v", i, err)
			}

			if deleted := idx.Delete(bali.U64(i), i); !deleted {
				t.Errorf("Error deleting value %d", i)
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkInsert(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	for i := 0; i < b.N; i++ {
		idx.Insert(bali.U64(i), uint64(i))
	}
}

func BenchmarkSearch(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	for i := 0; i < length; i++ {
		idx.Insert(bali.U64(i), uint64(i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = idx.Search(bali.U64(i))
	}
}

func BenchmarkDelete(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	for i := 0; i < length; i++ {
		idx.Insert(bali.U64(i), uint64(i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx.Delete(bali.U64(i), uint64(i))
	}
}

func BenchmarkConcurrentInsert(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	wg := new(sync.WaitGroup)

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			idx.Insert(bali.U64(i), uint64(i))
		}(i)
	}

	wg.Wait()
}

func BenchmarkConcurrentSearch(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	for i := 0; i < length; i++ {
		idx.Insert(bali.U64(i), uint64(i))
	}

	wg := new(sync.WaitGroup)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			_, _ = idx.Search(bali.U64(i))
		}(i)
	}

	wg.Wait()
}

func BenchmarkConcurrentDelete(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	for i := 0; i < length; i++ {
		idx.Insert(bali.U64(i), uint64(i))
	}

	wg := new(sync.WaitGroup)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			idx.Delete(bali.U64(i), uint64(i))
		}(i)
	}

	wg.Wait()
}

func BenchmarkSearchRange(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	indices := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	for _, i := range indices {
		idx.Insert(bali.U64(i), i)
	}

	start := uint64(150)
	end := uint64(9750)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx.SearchRange(bali.U64(start), bali.U64(end), func(recordID uint64) error {
			return nil
		})
	}
}

func BenchmarkRaceCondition(b *testing.B) {
	idx := bali.NewIndex[bali.U64](threshold)

	indices := make([]uint64, length)
	for i := uint64(0); i < length; i++ {
		indices[i] = i
	}
	for i := 0; i < length; i++ {
		j := rand.Intn(length)
		indices[i], indices[j] = indices[j], indices[i]
	}

	wg := new(sync.WaitGroup)

	b.ResetTimer()

	for _, i := range indices {
		if err := idx.Insert(bali.U64(i), i); err != nil {
			b.Errorf("Error inserting value %d: %v", i, err)
		}

		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()

			time.Sleep(1 * time.Millisecond)

			if _, err := idx.Search(bali.U64(i)); err != nil {
				b.Errorf("Error searching for value %d: %v", i, err)
			}

			if deleted := idx.Delete(bali.U64(i), i); !deleted {
				b.Errorf("Error deleting value %d", i)
			}
		}(i)
	}

	wg.Wait()
}
