package bitcask

import (
    "os"
)

type RandomAccessFile struct {
    name    string
    f       *os.File
    size    int64
    id      int64
}

func NewRandomAccessFile(name string, id int64, create bool) (*RandomAccessFile, error) {
    flags := os.O_RDWR
    if create {
        flags |= os.O_CREATE
    }
    f, err := os.OpenFile(name, flags, 0644)
    if err != nil {
        return nil, err
    }
    fi, err := f.Stat()
    if err != nil {
        return nil, err
    }

    f.Seek(0, os.SEEK_SET)
    raf := &RandomAccessFile {
        name: name,
        f: f,
        size: fi.Size(),
        id: id,
    }
    return raf, nil
}

func (raf *RandomAccessFile) ReadAt(offset int64, len int64) ([]byte, error) {
    p := make([]byte, len)
    _, err := raf.f.ReadAt(p, offset)
    return p, err
}

func (raf *RandomAccessFile) WriteAt(offset int64, p []byte) error {
    _, err := raf.f.WriteAt(p, offset)
    if err != nil {
        return err
    }
    fi, err := raf.f.Stat()
    if err != nil {
        return err
    }
    raf.size = fi.Size()
    return nil
}

func (raf *RandomAccessFile) Append(p []byte) error {
    _, err := raf.f.Seek(0, os.SEEK_END)
    n, err := raf.f.Write(p)
    if err != nil {
        return err
    }
    raf.size += int64(n)
    return nil
}

func (raf *RandomAccessFile) Seek(offset int64) error {
    _, err := raf.f.Seek(offset, os.SEEK_SET)
    return err
}

func (raf *RandomAccessFile) Size() int64 {
    return raf.size
}

func (raf *RandomAccessFile) Close() error {
    err := raf.f.Close()
    return err
}

