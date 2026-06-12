# 📦 Flow: Reactive State Management for Go

`go-flow` brings the ergonomics of frontend reactive stores (like Svelte stores or Zustand) to Go, strictly adhering to Go idioms. It provides a standardized, zero-dependency, concurrency-safe way to manage shared reactive state across long-running routines.

## Core Tenets

- **Zero Dependencies:** Relies entirely on the Go standard library.
- **Generics-First:** Fully type-safe. No `interface{}` casting.
- **Deadlock-Proof:** Built on a "Read-Copy-Notify" pattern that guarantees lock contention and deadlocks are impossible during subscriber notification.
- **Extensible:** First-class functional options and middleware support.

## Installation

```bash
go get github.com/on1force/go-flow
```

## Usage

### Basic Store

```go
package main

import (
	"fmt"
	"github.com/on1force/go-flow"
)

func main() {
	// Create a new store with initial state
	store := flow.New(0)

	// Subscribe to changes
	unsub := store.Subscribe(func(state int) {
		fmt.Printf("State is now: %d\n", state)
	})
	defer unsub()

	// Update state
	store.Set(5)
	
	store.Update(func(current int) int {
		return current * 2
	})
}
```

### Extensibility & Middlewares

You can plug into the state transition lifecycle using Middlewares.

```go
import (
	"github.com/on1force/go-flow"
	"github.com/on1force/go-flow/middleware"
)

func main() {
	// Use the built-in logger middleware
	store := flow.New("initial", flow.WithMiddleware(middleware.Logger[string]()))
	
	store.Set("updated") 
	// Output: 2026/06/13 12:00:00 State transitioned from initial to updated
}
```

### Derived Stores (Computed State)

Create a read-only projection of a parent store. When the parent updates, the derived store maps the new state and notifies its own subscribers.

```go
parentStore := flow.New(10)

// Create a derived store that returns true if the parent is > 50
childStore := flow.Derived(parentStore, func(p int) bool {
    return p > 50
})

childStore.Subscribe(func(isLarge bool) {
    fmt.Printf("Is Large? %v\n", isLarge)
})

parentStore.Set(100) // triggers the childStore subscriber with `true`
```

### Asynchronous Broadcasting

By default, `Set` and `Update` block until all subscribers are notified sequentially. For UI apps or heavy listeners, you can enable async broadcasting to fire each subscriber in its own goroutine.

```go
store := flow.New(0, flow.WithAsyncBroadcast[int]())
```

## Benchmarks

`go-flow` is designed for hot-path performance with near-zero allocations on the core loop.

*(Specs: 11th Gen Intel Core i5 @ 2.70GHz, Windows)*

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Get` | 13.1 ns/op | 0 B/op | 0 allocs/op |
| `Set` | 25.1 ns/op | 0 B/op | 0 allocs/op |
| `Set_WithSubscribers` | 64.7 ns/op | 80 B/op | 1 allocs/op |
| `Concurrent_RW` | 137 ns/op | 48 B/op | 1 allocs/op |

## License

MIT License
