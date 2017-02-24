package bitcask

import (
    "fmt"
    "sync"
)

var (
    ErrNotFound = fmt.Errorf("not found")
)

type BitCask struct {
    dir         string
    keyDir      *KeyDir
    activeFile  *DataFile
    mu          *sync.RWMutex
}

func Open(dir string, opts Options) (*BitCask, error) {
    return nil, nil
}

func (bc *BitCask) Get(key string) ([]byte, error) {
    return nil, nil
}

func (bc *BitCask) Set(key string, val []byte) error {
    return nil
}

func (bc *BitCask) Del(key string) error {
    return nil
}

func (bc *BitCask) ListKeys() ([]string, error) {
    return nil, nil
}

func (bc *BitCask) Merge() error {
    return nil
}

func (bc *BitCask) Sync() error {
    return nil
}

func (bc *BitCask) Close() error {
    return nil
}



