package middleware

import (
	"log"

	"github.com/on1force/go-flow"
)

// Logger returns a middleware that logs state transitions to standard logger.
func Logger[T any]() flow.Middleware[T] {
	return func(oldState, newState T) {
		log.Printf("State transitioned from %v to %v", oldState, newState)
	}
}
