package flow

// Derived creates a new ReadOnlyStore whose state is a mapped projection
// of a parent ReadOnlyStore. It updates automatically when the parent updates.
func Derived[Parent any, Child any](parent ReadOnlyStore[Parent], mapper func(Parent) Child) ReadOnlyStore[Child] {
	// Map the initial state
	initialState := mapper(parent.Get())

	// Create the child store
	childStore := New(initialState)

	// Subscribe to the parent
	// Note: We don't necessarily want to fire the mapped initial state right away again
	// if Subscribe fires immediately. Because we already initialized childStore with it.
	// But childStore.Set will just overwrite it and notify its subscribers,
	// which currently is empty right here.
	parent.Subscribe(func(parentState Parent) {
		childStore.Set(mapper(parentState))
	})

	// Return as ReadOnlyStore
	return childStore
}
