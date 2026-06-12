package flow_test

import (
	"testing"

	"github.com/on1force/go-flow"
)

func BenchmarkStore_Get(b *testing.B) {
	store := flow.New(10)

	for b.Loop() {
		_ = store.Get()
	}
}

func BenchmarkStore_Set(b *testing.B) {
	store := flow.New(10)

	for i := 0; b.Loop(); i++ {
		store.Set(i)
	}
}

func BenchmarkStore_Set_WithSubscribers(b *testing.B) {
	store := flow.New(10)
	for range 10 {
		store.Subscribe(func(val int) {})
	}

	for i := 0; b.Loop(); i++ {
		store.Set(i)
	}
}

func BenchmarkStore_Update(b *testing.B) {
	store := flow.New(10)

	for b.Loop() {
		store.Update(func(current int) int {
			return current + 1
		})
	}
}

func BenchmarkStore_Concurrent_RW(b *testing.B) {
	store := flow.New(10)

	// Simulate some subscribers
	for range 5 {
		store.Subscribe(func(val int) {})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Get()
			store.Update(func(current int) int {
				return current + 1
			})
		}
	})
}

func BenchmarkStore_Async_Concurrent_RW(b *testing.B) {
	store := flow.New(10, flow.WithAsyncBroadcast[int]())

	// Simulate some subscribers
	for range 5 {
		store.Subscribe(func(val int) {
			// Do a tiny bit of work to simulate rendering/UI update
			_ = val * 2
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Every update fires 5 subscribers asynchronously
			store.Update(func(current int) int {
				return current + 1
			})
		}
	})
}
