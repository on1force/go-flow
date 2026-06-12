package flow_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/on1force/go-flow"
)

func TestStore_GetSet(t *testing.T) {
	store := flow.New(10)

	if got := store.Get(); got != 10 {
		t.Errorf("expected 10, got %d", got)
	}

	store.Set(20)

	if got := store.Get(); got != 20 {
		t.Errorf("expected 20, got %d", got)
	}
}

func TestStore_Update(t *testing.T) {
	store := flow.New(10)

	store.Update(func(current int) int {
		return current * 2
	})

	if got := store.Get(); got != 20 {
		t.Errorf("expected 20, got %d", got)
	}
}

func TestStore_Subscribe(t *testing.T) {
	store := flow.New(0)

	var lastVal int
	var callCount int

	unsub := store.Subscribe(func(state int) {
		lastVal = state
		callCount++
	})

	// Should be called immediately on subscribe
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
	if lastVal != 0 {
		t.Errorf("expected 0, got %d", lastVal)
	}

	store.Set(5)
	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
	if lastVal != 5 {
		t.Errorf("expected 5, got %d", lastVal)
	}

	unsub()

	store.Set(10)
	if callCount != 2 {
		t.Errorf("expected 2 calls after unsub, got %d", callCount)
	}
}

func TestStore_DeadlockPrevention(t *testing.T) {
	// If the store holds a lock while calling a subscriber,
	// calling Set or Get from within the subscriber will cause a deadlock.
	store := flow.New(0)

	done := make(chan bool)

	store.Subscribe(func(state int) {
		switch state {
		case 1:
			// This would deadlock if the mutex was still held
			store.Get()
			store.Set(2)
		case 2:
			done <- true
		}
	})

	// Trigger the first state change
	go func() {
		store.Set(1)
	}()

	select {
	case <-done:
		// Success, no deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock detected or timeout")
	}
}

func TestStore_RaceAndConcurrency(t *testing.T) {
	// Rigorous test for race conditions using many goroutines
	store := flow.New(0)

	const numWriters = 100
	const numReaders = 100
	const opsPerWorker = 1000

	var wg sync.WaitGroup
	wg.Add(numWriters + numReaders)

	// Keep track of subscriber execution safely
	var subExecutions atomic.Int64

	unsub := store.Subscribe(func(state int) {
		subExecutions.Add(1)
	})
	defer unsub()

	// Writers
	for range numWriters {
		go func() {
			defer wg.Done()
			for range opsPerWorker {
				store.Update(func(current int) int {
					return current + 1
				})
			}
		}()
	}

	// Readers
	for range numReaders {
		go func() {
			defer wg.Done()
			for range opsPerWorker {
				_ = store.Get()
			}
		}()
	}

	wg.Wait()

	finalVal := store.Get()
	expected := numWriters * opsPerWorker
	if finalVal != expected {
		t.Errorf("expected %d, got %d", expected, finalVal)
	}

	// Just checking that subExecutions > 0, since many updates occurred
	if subExecutions.Load() == 0 {
		t.Errorf("expected subscribers to be called")
	}
}

func TestStore_Middleware(t *testing.T) {
	var oldStates []int
	var newStates []int

	mw := func(oldState, newState int) {
		oldStates = append(oldStates, oldState)
		newStates = append(newStates, newState)
	}

	store := flow.New(10, flow.WithMiddleware(mw))

	store.Set(20)
	store.Update(func(current int) int {
		return current + 5
	})

	if len(oldStates) != 2 || len(newStates) != 2 {
		t.Fatalf("expected 2 middleware calls, got %d", len(oldStates))
	}

	if oldStates[0] != 10 || newStates[0] != 20 {
		t.Errorf("expected transition 10 -> 20, got %d -> %d", oldStates[0], newStates[0])
	}

	if oldStates[1] != 20 || newStates[1] != 25 {
		t.Errorf("expected transition 20 -> 25, got %d -> %d", oldStates[1], newStates[1])
	}
}

func TestStore_AsyncBroadcast(t *testing.T) {
	store := flow.New(10, flow.WithAsyncBroadcast[int]())

	var wg sync.WaitGroup
	wg.Add(1)

	// Since subscription fires immediately, and the initial fire is synchronous,
	// the first call happens before subscribe returns.
	firstCall := true

	store.Subscribe(func(state int) {
		if firstCall {
			firstCall = false
			return
		}
		if state == 20 {
			wg.Done()
		}
	})

	store.Set(20)

	// Wait for the async callback
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for async broadcast")
	}
}

func TestStore_Derived(t *testing.T) {
	parent := flow.New(5)

	child := flow.Derived(parent, func(p int) string {
		if p > 10 {
			return "high"
		}
		return "low"
	})

	if child.Get() != "low" {
		t.Errorf("expected low, got %s", child.Get())
	}

	var lastVal string
	child.Subscribe(func(c string) {
		lastVal = c
	})

	// Subscribing immediately updates lastVal
	if lastVal != "low" {
		t.Errorf("expected low, got %s", lastVal)
	}

	parent.Set(15)

	if child.Get() != "high" {
		t.Errorf("expected high, got %s", child.Get())
	}
	if lastVal != "high" {
		t.Errorf("expected high, got %s", lastVal)
	}
}
