package bitcask

import (
    github.com/rocket323/bitcask/lru
)

type DataFile struct {
    fr  FileReader
    id  int64
}

func NewDataFile(path string, fileId int64) (*DataFile, error) {
    f, err := NewFileWithBuffer(path, false, 1000)
    if err != nil {
        return nil, err
    }
    df := &DataFile{
        fr: f,
        id: fileId,
    }
    return df, nil
}

type DataFileCache struct {
    cache       *lru.Cache
    capacity    int64
}

func NewDataFileCache(int capacity) *DataFileCache {
    onEvit := func(k interface{}, v interface{}) {
        v.(*DataFile).Close()
    }

    c := lru.NewCache(capacity, onEvit)
    dfc: = &DataFileCache{
        cache: c,
        capacity: capacity,
    }
    return dfc
}

func (c *DataFileCache) Ref(path string, fileId int64) (*DataFile, error) {
    v, err := c.cache.Ref(fileId)
    if err == nil {
        return v.(*DataFile), nil
    }

    df, err := NewDataFile(path, fileId)
    if err != nil {
        return nil, err
    }
    c.cache.Put(fileId, df)
    c.cache.Ref(fileId)
}

func (c *DataFileCache) Unref(fileId int64) error {
    return c.cache.Unref(fileId)
}

func (c *DataFileCache) Close() {
    return c.cache.Close()
}

