package bitcask

import (
    "os"
    "log"
)

type RandomAccessFile struct {
    f       *os.File
    size    int64
}

func NewRandomAccessFile(file string, bool create) (*RandomAccessFile, error) {
    flags := os.O_RDWR
    if create {
        flags |= os.O_CREATE
    }
    f, err := os.OpenFile(file, flags, 0644)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    fi, err := f.Stat()
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    raf := &RandomAccessFile {
        f: f,
        size: fi.Size(),
    }
    return raf, nil
}

func (raf *RandomAccessFile) ReadAt(offset int64, len int64) ([]byte, error) {
    p := make([]byte, len)
    _, err := raf.f.ReadAt(p, offset)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }
    return p, nil
}

func (raf *RandomAccessFile) WriteAt(offset int64, p []byte) error {
    _, err := raf.f.WriteAt(p, offset)
    if err != nil {
        log.Fatal(err)
        return err
    }
    fi, err := raf.f.Stat()
    if err != nil {
        log.Fatal(err)
        return err
    }
    raf.size = fi.Size()
    return nil
}

func (raf *RandomAccessFile) Append(p []byte) error {
    n, err := raf.f.Write(p)
    if err != nil {
        log.Fatal(err)
        return err
    }
    raf.size += int64(n)
    return nil
}

func (raf *RandomAccessFile) Size() int64 {
    return raf.size
}

func (raf *RandomAccessFile) Close() error {
    err := raf.f.Close()
    if err != nil {
        return err
    }
    return nil
}


