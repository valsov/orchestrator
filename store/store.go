package store

import "errors"

var ErrKeyNotFound = errors.New("key not found")

type Store[TKey, TVal any] interface {
	List() ([]TVal, error)
	Count() (int, error)
	Get(key TKey) (TVal, error)
	Put(key TKey, value TVal) error
	Close() error
}
