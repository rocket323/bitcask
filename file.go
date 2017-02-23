package bitcask

import (
    os
)

type RandomAccessFile interface {
    Append(data []byte) error
    ReadAt(offset int64, len int64) ([]byte, error)
    Size() int64
    Close() error
}

type MmapFile struct {
}

type DiskFile struct {
    f       *os.File
    size    int64
}

func NewDiskFile(fn string) (*NewDiskFile, error) {
    f, err := os.OpenFile(fn, os.O_RDWR | os.O_CREATE, 0644)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    df := &DiskFile {
        f: f
        size: 0
    }

    return df, nil
}

