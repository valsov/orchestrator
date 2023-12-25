package store

import (
	"encoding/json"
	"fmt"
	"io/fs"

	bolt "go.etcd.io/bbolt"
)

type PersistedStore[TKey fmt.Stringer, TVal any] struct {
	Db         *bolt.DB
	BucketName string
}

func NewPersistedStore[TKey fmt.Stringer, TVal any](file string, mode fs.FileMode, storeName string) (*PersistedStore[TKey, TVal], error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(storeName))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &PersistedStore[TKey, TVal]{
		Db:         db,
		BucketName: storeName,
	}, err
}

func (s *PersistedStore[TKey, TVal]) List() ([]TVal, error) {
	items := []TVal{}
	err := s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.BucketName))
		if b == nil {
			return fmt.Errorf("bucket with name %s doesn't exist", s.BucketName)
		}

		cur := b.Cursor()
		var err error
		for key, jsonVal := cur.First(); key != nil; _, jsonVal = cur.Next() {
			var value TVal
			err = json.Unmarshal(jsonVal, &value)
			if err != nil {
				items = append(items, value)
			}
		}
		return err
	})
	return items, err
}

func (s *PersistedStore[TKey, TVal]) Count() (int, error) {
	var count int
	err := s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.BucketName))
		if b == nil {
			return fmt.Errorf("bucket with name %s doesn't exist", s.BucketName)
		}
		count = b.Stats().KeyN
		return nil
	})
	return count, err
}

func (s *PersistedStore[TKey, TVal]) Get(key TKey) (TVal, error) {
	var value TVal
	err := s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.BucketName))
		if b == nil {
			return fmt.Errorf("bucket with name %s doesn't exist", s.BucketName)
		}

		jsonVal := b.Get([]byte(key.String()))
		if jsonVal == nil {
			return fmt.Errorf("value with key %s not found", key)
		}

		return json.Unmarshal(jsonVal, &value)
	})
	return value, err
}

func (s *PersistedStore[TKey, TVal]) Put(key TKey, value TVal) error {
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.BucketName))
		if b == nil {
			return fmt.Errorf("bucket with name %s doesn't exist", s.BucketName)
		}

		jsonVal, err := json.Marshal(value)
		if err != nil {
			return err
		}

		return b.Put([]byte(key.String()), jsonVal)
	})
	return err
}

func (s *PersistedStore[TKey, TVal]) Close() error {
	return s.Db.Close()
}
