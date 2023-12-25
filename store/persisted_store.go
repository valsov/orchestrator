package store

// bbolt
type PersistedStore[TKey, TVal any] struct{}

func NewPersistedStore[TKey, TVal any]() *PersistedStore[TKey, TVal] {
	panic("TODO: Implement")
}

func (t *PersistedStore[TKey, TVal]) List() ([]TVal, error) {
	panic("TODO: Implement")
}

func (t *PersistedStore[TKey, TVal]) Count() (int, error) {
	panic("TODO: Implement")
}

func (t *PersistedStore[TKey, TVal]) Get(key TKey) (TVal, error) {
	panic("TODO: Implement")
}

func (t *PersistedStore[TKey, TVal]) Put(key TKey, value TVal) error {
	panic("TODO: Implement")
}
