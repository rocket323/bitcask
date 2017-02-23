package bitcask

import (
    fmt
)

var (
    ErrNotFound = fmt.Errorf("not found")
)

struct BitCask {
    dir         string,
    keyDir      KeyDir,
    activeFile  DataFile,
}

func Open(dir string, opts options) (*BitCask, error) {
}

func Open(dir string) (*BitCask, error) {
}

func (bc *BitCask) Get(key string) ([]byte, error) {
}

func (bc *BitCask) Set(key string, val []byte) error {
}

func (bc *BitCask) Del(key string) error {
}

func (bc *BitCask) ListKeys() ([]string, error) {
}

func (bc *BitCask) Merge() error {
}

func (bc *BitCask) Sync() error {
}

func (bc *BitCask) Close() error {
}



