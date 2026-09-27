package utils

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestBufferPool_HappyPath_GetAndPut(t *testing.T) {
	pool := NewBufferPool[int]()
	buf := pool.Get()
	if buf == nil {
		t.Fatal("expected non-nil buffer from Get")
	}
	if len(*buf) != 0 {
		t.Errorf("expected empty slice length, got %d", len(*buf))
	}
	if cap(*buf) < 128 {
		t.Errorf("expected slice capacity at least 128, got %d", cap(*buf))
	}
	*buf = append(*buf, 10, 20, 30)
	if len(*buf) != 3 {
		t.Fatalf("expected length 3, got %d", len(*buf))
	}
	pool.Put(buf)

	buf2 := pool.Get()
	if len(*buf2) != 0 {
		t.Errorf("expected recycled buffer length to be reset to 0, got %d", len(*buf2))
	}
}

func TestBufferPool_SadPath_NilAndOversized(t *testing.T) {
	pool := NewBufferPool[byte]()

	t.Run("Nil Put", func(t *testing.T) {
		// Should not panic
		pool.Put(nil)
	})

	t.Run("Oversized Buffer Dropped", func(t *testing.T) {
		// Force GC to clear pool first
		runtime.GC()

		oversizedBuf := make([]byte, 0, 10001)
		oversizedBuf = append(oversizedBuf, 'X')

		pool.Put(&oversizedBuf)
		retrieved := pool.Get()
		if cap(*retrieved) > 10000 {
			t.Errorf("oversized buffer was put into pool; expected capacity <= 128, got %d", cap(*retrieved))
		}
	})
}

func TestBufferPool_PoisonPill_DataIsolation(t *testing.T) {
	type SensitiveData struct {
		SecretKey string
		IsPoison  bool
	}

	pool := NewBufferPool[SensitiveData]()
	buf1 := pool.Get()
	*buf1 = append(*buf1, SensitiveData{
		SecretKey: "MALICIOUS_POISON_PAYLOAD",
		IsPoison:  true,
	})
	pool.Put(buf1)
	buf2 := pool.Get()
	if len(*buf2) != 0 {
		t.Fatalf("poison leaked into active length: expected 0, got %d", len(*buf2))
	}
	*buf2 = append(*buf2, SensitiveData{SecretKey: "SAFE_DATA", IsPoison: false})
	if (*buf2)[0].IsPoison {
		t.Errorf("poison pill leaked through re-slice operation")
	}
}

func TestBufferPool_ConcurrentStress(t *testing.T) {
	pool := NewBufferPool[int]()
	var wg sync.WaitGroup

	workers := 50
	iterations := 1000

	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range iterations {
				buf := pool.Get()
				if len(*buf) != 0 {
					t.Errorf("worker %d: got dirty buffer with length %d", workerID, len(*buf))
				}
				*buf = append(*buf, workerID, j)
				if j%200 == 0 {
					bigBuf := make([]int, 0, 15000)
					pool.Put(&bigBuf)
				} else {
					pool.Put(buf)
				}
			}
		}(i)
	}

	wg.Wait()
}

// --- MapPool Tests ---

func TestMapPool_HappyPath_GetAndPut(t *testing.T) {
	pool := NewMapPool[string, int]()

	m := pool.Get()
	if m == nil {
		t.Fatal("expected non-nil map from Get")
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got len %d", len(m))
	}

	m["foo"] = 100
	m["bar"] = 200

	pool.Put(m)

	m2 := pool.Get()
	if len(m2) != 0 {
		t.Errorf("expected map to be cleared on reuse, got len %d", len(m2))
	}
	if _, exists := m2["foo"]; exists {
		t.Errorf("expected key 'foo' to be cleared from recycled map")
	}
}

func TestMapPool_SadPath_NilAndOversized(t *testing.T) {
	pool := NewMapPool[int, string]()

	t.Run("Nil Put", func(t *testing.T) {
		// Should not panic
		pool.Put(nil)
	})

	t.Run("Oversized Map Dropped", func(t *testing.T) {
		runtime.GC()
		m := pool.Get()
		for i := 0; i <= 10000; i++ {
			m[i] = "overflow"
		}
		pool.Put(m)
		m2 := pool.Get()
		if len(m2) != 0 {
			t.Errorf("expected empty map from New(), got len %d", len(m2))
		}
	})
}

func TestMapPool_PoisonPill_KeyLeakPrevention(t *testing.T) {
	pool := NewMapPool[string, any]()

	m1 := pool.Get()

	// Inject poison key and corrupt values
	m1["POISON_KEY"] = func() { panic("poison pill executed") }
	m1["CORRUPT_STATE"] = []byte{0xDE, 0xAD, 0xBE, 0xEF}

	pool.Put(m1)

	// Fetch map back
	m2 := pool.Get()

	if len(m2) != 0 {
		t.Fatalf("map was not cleared properly, contained %d keys", len(m2))
	}
	if _, found := m2["POISON_KEY"]; found {
		t.Errorf("poison key persisted across map reuse")
	}
}

func TestMapPool_ConcurrentStress(t *testing.T) {
	pool := NewMapPool[string, int]()
	var wg sync.WaitGroup
	workers := 50
	iterations := 500
	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range iterations {
				m := pool.Get()

				if len(m) != 0 {
					t.Errorf("worker %d: got non-empty map with len %d", workerID, len(m))
				}
				key := fmt.Sprintf("w_%d_k_%d", workerID, j)
				m[key] = j
				if j%100 == 0 {
					for k := 0; k <= 10001; k++ {
						m[fmt.Sprintf("overflow_%d", k)] = k
					}
				}
				pool.Put(m)
			}
		}(i)
	}

	wg.Wait()
}
