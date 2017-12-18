package storage

import (
	"errors"
)

var (
	KMissingError    = errors.New("No such key")
	KNilNodeError    = errors.New("Etcd Store returns a nil node")
	KDirNodeError    = errors.New("Etcd Store returns this is a directory node")
	KNonDirNodeError = errors.New("Etcd Store returns this is a non-directory node")
)

type Store interface {
	Get(key string, v interface{}) error
	Set(key string, v interface{}, force ...bool) error
	SetWithTTL(key string, v interface{}, ttlSec int, force ...bool) error
	Watch(key string) chan string
	KeysByPrefix(prefix string) ([]string, error)
	Remove(key string) error
	TryRemoveDir(key string)
	RemoveDir(key string) error
}
