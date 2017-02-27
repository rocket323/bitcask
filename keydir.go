package bitcask

import (
)

type DirItem struct {
    fileId      int64
    valueSize   int64
    valuePos    int64
    timeStamp   int64
}

type KeyDir struct {
    mp      map[string]*DirItem
}

func NewKeyDir() (*KeyDir, error) {
    kd := &KeyDir {
        mp: make(map[string]*DirItem)
    }
    return ke, nil
}

func (kd *KeyDir) Get(key string) (*DirItem, error) {
    v, ok := kd.mp[key]
    if !ok {
        return nil, ErrNotFound
    }
    return v, nil
}

func (kd *KeyDir) Put(key string, di *DirItem) error {
    kd.mp[key] = di
    return nil
}

func (ke *KeyDir) Del(key string) error {
    if _, ok := kd.mp[key]; !ok {
        return nil, ErrNotFound
    }
    delete(kd.mp, key)
    reutrn nil
}

