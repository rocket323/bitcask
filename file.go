package bitcask

import (
    "os"
    "log"
    "io"
)

type RandomAccessFile struct {
    f       *os.File
    size    int64
    id      int64
}

func NewRandomAccessFile(dir string, id int64, create bool) (*RandomAccessFile, error) {
    flags := os.O_RDWR
    if create {
        flags |= os.O_CREATE
    }
    name := dir + "/" + GetFileBaseName(id) + ".data"

    f, err := os.OpenFile(name, flags, 0644)
    if err != nil {
        log.Println(err)
        return nil, err
    }
    fi, err := f.Stat()
    if err != nil {
        log.Println(err)
        return nil, err
    }

    f.Seek(0, os.SEEK_SET)
    raf := &RandomAccessFile {
        f: f,
        size: fi.Size(),
        id: id,
    }
    return raf, nil
}

func (raf *RandomAccessFile) Seek(offset int64) error {
    _, err := raf.f.Seek(offset, os.SEEK_SET)
    if err != nil {
        return err
    }
    return nil
}

func (raf *RandomAccessFile) ReadAt(offset int64, len int64) ([]byte, error) {
    p := make([]byte, len)
    _, err := raf.f.ReadAt(p, offset)
    if err != nil {
        if err != io.EOF {
            log.Println(err)
        }
        return nil, err
    }
    return p, nil
}

func (raf *RandomAccessFile) WriteAt(offset int64, p []byte) error {
    _, err := raf.f.WriteAt(p, offset)
    if err != nil {
        log.Println(err)
        return err
    }
    fi, err := raf.f.Stat()
    if err != nil {
        log.Println(err)
        return err
    }
    raf.size = fi.Size()
    return nil
}

func (raf *RandomAccessFile) Append(p []byte) error {
    _, err := raf.f.Seek(0, os.SEEK_END)
    n, err := raf.f.Write(p)
    if err != nil {
        log.Println(err)
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


