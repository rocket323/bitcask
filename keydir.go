package bitcask

import (
)

type DirItem struct {
    flag        uint8
    fileId      int64
    valuePos    int64
    valueSize   int64
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

func (kd *KeyDir) Get(key []byte) (*DirItem, error) {
    v, ok := kd.mp[string(key)]
    if !ok {
        return nil, ErrKeyNotFound
    }
    return v, nil
}

func (kd *KeyDir) Put(key []byte, di *DirItem) error {
    kd.mp[string(key)] = di
    return nil
}

func (kd *KeyDir) Del(key []byte) error {
    if _, ok := kd.mp[string(key)]; !ok {
        return ErrKeyNotFound
    }
    delete(kd.mp, string(key))
    return nil
}

func (kd *KeyDir) Clear() {
    kd.mp = make(map[string]*DirItem)
}

