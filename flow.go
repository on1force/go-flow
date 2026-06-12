// Package flow provides a generic, zero-dependency, and deadlock-proof
// reactive state management library for Go. It allows you to create stores,
// subscribe to state changes, and safely manage concurrent state transitions.
package flow

import (
	"sync"
	"sync/atomic"
)

// Listener is the callback triggered on state changes.
type Listener[T any] func(state T)

// Unsubscribe removes a listener.
type Unsubscribe func()

// ReadOnlyStore is a restricted interface for state management.
type ReadOnlyStore[T any] interface {
	Get() T
	Subscribe(listener Listener[T]) Unsubscribe
}

// Store is the primary interface for state management.
type Store[T any] interface {
	ReadOnlyStore[T]
	Set(val T)
	Update(fn func(current T) T)
}

// listenerEntry is an internal struct to track listeners with a unique ID.
type listenerEntry[T any] struct {
	id int64
	cb Listener[T]
}

type flowStore[T any] struct {
	mu      sync.RWMutex
	value   T
	subs    []listenerEntry[T]
	nextID  atomic.Int64
	options Options[T]
}

// New creates a new Store with the given initial state.
func New[T any](initialState T, opts ...Option[T]) Store[T] {
	options := Options[T]{}
	for _, opt := range opts {
		opt(&options)
	}

	return &flowStore[T]{
		value:   initialState,
		subs:    make([]listenerEntry[T], 0),
		options: options,
	}
}

func (f *flowStore[T]) Get() T {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.value
}

func (f *flowStore[T]) Set(val T) {
	f.mu.Lock()
	oldVal := f.value
	f.value = val

	// Read-Copy-Notify: Copy callbacks to a local slice while holding the lock
	callbacks := make([]Listener[T], len(f.subs))
	for i, entry := range f.subs {
		callbacks[i] = entry.cb
	}
	f.mu.Unlock() // FREE THE LOCK before notifying

	// Run Middlewares outside the lock
	for _, mw := range f.options.Middlewares {
		mw(oldVal, val)
	}

	// Fire callbacks safely outside the lock
	if f.options.AsyncBroadcast {
		for _, cb := range callbacks {
			go cb(val)
		}
	} else {
		for _, cb := range callbacks {
			cb(val)
		}
	}
}

func (f *flowStore[T]) Update(fn func(current T) T) {
	f.mu.Lock()
	oldVal := f.value
	newVal := fn(f.value)
	f.value = newVal

	// Read-Copy-Notify
	callbacks := make([]Listener[T], len(f.subs))
	for i, entry := range f.subs {
		callbacks[i] = entry.cb
	}
	f.mu.Unlock()

	// Run Middlewares outside the lock
	for _, mw := range f.options.Middlewares {
		mw(oldVal, newVal)
	}

	if f.options.AsyncBroadcast {
		for _, cb := range callbacks {
			go cb(newVal)
		}
	} else {
		for _, cb := range callbacks {
			cb(newVal)
		}
	}
}

func (f *flowStore[T]) Subscribe(listener Listener[T]) Unsubscribe {
	f.mu.Lock()

	// Issue a unique ID for this subscriber
	id := f.nextID.Add(1)
	f.subs = append(f.subs, listenerEntry[T]{id: id, cb: listener})

	// Important: We must not hold the lock while notifying the immediate state
	// But it is common in reactive stores to fire the listener immediately with current state.
	// Wait, the plan didn't explicitly mention immediate firing upon subscribe,
	// but it's a common pattern (like in Svelte stores). Let's stick to standard notify on update for now,
	// or fire immediately? Svelte fires immediately. Let's fire immediately outside the lock.
	currentVal := f.value
	f.mu.Unlock()

	// We notify the new subscriber with the current state immediately upon subscription.
	listener(currentVal)

	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()

		// Find and remove the listener by ID
		for i, entry := range f.subs {
			if entry.id == id {
				// Remove without preserving order to be fast
				// Or preserve order if it matters? Usually order doesn't matter much.
				// Let's preserve order to be safe and predictable.
				f.subs = append(f.subs[:i], f.subs[i+1:]...)
				break
			}
		}
	}
}
