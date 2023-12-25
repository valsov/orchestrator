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

func (t *MemoryStore[TKey, TVal]) List() ([]TVal, error) {
	tasks := make([]TVal, len(t.Db))
	for _, storedTask := range t.Db {
		tasks = append(tasks, storedTask)
	}
	return tasks, nil
}

func (t *MemoryStore[TKey, TVal]) Count() (int, error) {
	return len(t.Db), nil
}

func (t *MemoryStore[TKey, TVal]) Get(key TKey) (TVal, error) {
	storedTask, found := t.Db[key]
	if !found {
		var defaultVal TVal
		return defaultVal, fmt.Errorf("item with key %v not found", key)
	}
	return storedTask, nil
}

func (t *MemoryStore[TKey, TVal]) Put(key TKey, value TVal) error {
	t.Db[key] = value
	return nil
}
