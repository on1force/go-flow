package flow

// Middleware intercepts state changes.
type Middleware[T any] func(oldState, newState T)

// Options holds the configuration for a Store.
type Options[T any] struct {
	AsyncBroadcast bool
	Middlewares    []Middleware[T]
}

// Option applies a configuration to Options.
type Option[T any] func(*Options[T])

// WithAsyncBroadcast enables asynchronous notification of subscribers.
// This is a placeholder for Phase 3.
func WithAsyncBroadcast[T any]() Option[T] {
	return func(o *Options[T]) {
		o.AsyncBroadcast = true
	}
}

// WithMiddleware appends a middleware to the execution pipeline.
func WithMiddleware[T any](mw Middleware[T]) Option[T] {
	return func(o *Options[T]) {
		o.Middlewares = append(o.Middlewares, mw)
	}
}
