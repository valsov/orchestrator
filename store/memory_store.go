package store

import (
	"fmt"
)

type MemoryStore[TKey comparable, TVal any] struct {
	Db map[TKey]TVal
}

func NewMemoryStore[TKey comparable, TVal any]() *MemoryStore[TKey, TVal] {
	return &MemoryStore[TKey, TVal]{map[TKey]TVal{}}
}

func (s *MemoryStore[TKey, TVal]) List() ([]TVal, error) {
	tasks := make([]TVal, len(s.Db))
	for _, storedTask := range s.Db {
		tasks = append(tasks, storedTask)
	}
	return tasks, nil
}

func (s *MemoryStore[TKey, TVal]) Count() (int, error) {
	return len(s.Db), nil
}

func (s *MemoryStore[TKey, TVal]) Get(key TKey) (TVal, error) {
	storedTask, found := s.Db[key]
	if !found {
		var defaultVal TVal
		return defaultVal, fmt.Errorf("item with key %v not found", key)
	}
	return storedTask, nil
}

func (s *MemoryStore[TKey, TVal]) Put(key TKey, value TVal) error {
	s.Db[key] = value
	return nil
}

func (s *MemoryStore[TKey, TVal]) Close() error {
	return nil
}
