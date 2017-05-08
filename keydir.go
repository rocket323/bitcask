package bitcask

import (
)

type DirItem struct {
    fileId      int64
    valuePos    uint32
    valueSize   uint32
    expration   uint32
}

type KeyDir struct {
    mp      map[string]*DirItem
}

func NewKeyDir() *KeyDir {
    kd := &KeyDir {
        mp: make(map[string]*DirItem),
    }
    return kd
}

func (kd *KeyDir) Get(key string) (*DirItem, error) {
    v, ok := kd.mp[key]
    if !ok {
        return nil, ErrKeyNotFound
    }
    return v, nil
}

func (kd *KeyDir) Put(key string, di *DirItem) error {
    kd.mp[key] = di
    return nil
}

func (kd *KeyDir) Del(key string) error {
    if _, ok := kd.mp[key]; !ok {
        return ErrKeyNotFound
    }
    delete(kd.mp, key)
    return nil
}

func (kd *KeyDir) Clear() {
    kd.mp = make(map[string]*DirItem)
}

