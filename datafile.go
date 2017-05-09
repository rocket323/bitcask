package bitcask

import (
    "io"
    "github.com/rocket323/bitcask/lru"
)

type DataFile struct {
    FileReader
    id  int64
}

func NewDataFile(path string, fileId int64) (*DataFile, error) {
    f, err := NewFileWithBuffer(path, false, 1000)
    if err != nil {
        return nil, err
    }
    df := &DataFile{
        f,
        fileId,
    }
    return df, nil
}

func (df *DataFile) ForEachItem(fn func(rec *Record, offset int64) error) error {
    var offset int64 = 0
    for {
        rec, err := parseRecordAt(df, offset)
        if err != nil {
            if err == io.EOF {
                break
            }
            return err
        }

        err = fn(rec, offset)
        if err != nil {
            return err
        }
        offset += rec.Size()
    }

    return nil
}

///////////////////////////////////

type DataFileCache struct {
    cache       *lru.Cache
    capacity    int
    env         Env
}

func NewDataFileCache(env Env) *DataFileCache {
    onEvit := func(k interface{}, v interface{}) {
        v.(*DataFile).Close()
    }
    opts := env.getOptions()
    c := lru.NewCache(int(opts.maxOpenFiles), onEvit)
    dfc := &DataFileCache{
        cache: c,
        capacity: int(opts.maxOpenFiles),
        env: env,
    }
    return dfc
}

func (c *DataFileCache) Ref(fileId int64) (*DataFile, error) {
    v, err := c.cache.Ref(fileId)
    if err == nil {
        return v.(*DataFile), nil
    }
    env := c.env

    path := env.GetDataFilePath(fileId)
    df, err := NewDataFile(path, fileId)
    if err != nil {
        return nil, err
    }
    c.cache.Put(fileId, df)
    c.cache.Ref(fileId)
    return df, nil
}

func (c *DataFileCache) Unref(fileId int64) {
    c.cache.Unref(fileId)
}

func (c *DataFileCache) Close() {
    c.cache.Close()
}

