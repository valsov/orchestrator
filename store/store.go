package store

import "errors"

var ErrKeyNotFound = errors.New("key not found")

// Generic Key/Value data store
type Store[TKey, TVal any] interface {
	// Retrieve all stored values
	List() ([]TVal, error)

	// Count all stored values
	Count() (int, error)

	// Get the value associated with the given key
	//
	// Check if error is store.ErrKeyNotFound to differentiate from technical errors
	Get(key TKey) (TVal, error)

	// Create or update the value associated with the given key
	Put(key TKey, value TVal) error

	// Close the store
	Close() error
}
