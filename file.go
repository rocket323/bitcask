package bitcask

import (
    "os"
    "io"
    "log"
)

type FileReader interface {
    ReadAt(offset int64, len int64) ([]byte, error)
    Size() int64
    Close() error
    Path() string
}

type FileWithBuffer struct {
    path        string
    f           *os.File
    size        int64
    wbuf        []byte      // write buffer
    n           int         // bytes buffered in wbuf
}

func NewFileWithBuffer(path string, create bool, wbufSize int64) (*FileWithBuffer, error) {
    flags := os.O_RDWR
    if create {
        flags |= os.O_CREATE
    }
    f, err := os.OpenFile(path, flags, 0644)
    if err != nil {
        return nil, err
    }
    fi, err := f.Stat()
    if err != nil {
        return nil, err
    }
    fb := &FileWithBuffer{
        path: path,
        f: f,
        size: fi.Size(),
        wbuf: make([]byte, wbufSize),
        n: 0,
    }
    return fb, nil
}

func (f *FileWithBuffer) ReadAt(offset int64, len int64) ([]byte, error) {
    data := make([]byte, len)
    fileSize := f.size - int64(f.n)     // size of file, (buffer size excluded)
    var nn int = 0                      // bytes we have readed
    var err error

    if offset < fileSize {
        if offset + len <= fileSize {
            nn = int(len)
        } else {
            nn = int(fileSize - offset)
        }
        nn, err = f.f.ReadAt(data[:nn], offset)
        if err != nil {
            return data[:nn], err
        }
        offset = 0
    } else {
        offset -= fileSize
    }
    if offset < int64(f.n) {
        nn += copy(data[nn:], f.wbuf[offset:f.n])
    }

    if int64(nn) < len {
        log.Printf("totalSize[%d], fileSize[%d], expect len[%d], actual[%d]", f.size, fileSize, len, nn)
        err = io.EOF
    }
    return data[:nn], err
}

func (f *FileWithBuffer) Write(data []byte) (nn int, err error) {
    for len(data) > len(f.wbuf) - int(f.n) && err == nil {
        var n int
        if f.n == 0 {
            n, err = f.f.Write(data)
        } else {
            n = copy(f.wbuf[f.n:], data)
            f.n += n
            err = f.Flush()
        }
        nn += n
        data = data[n:]
    }
    if err != nil {
        f.size += int64(nn)
        return
    }
    n := copy(f.wbuf[f.n:], data)
    f.n += n
    nn += n
    f.size += int64(nn)
    if err != nil {
        log.Fatal("write file failed, err=", err)
    }
    return
}

func (f *FileWithBuffer) Flush() error {
    if f.n == 0 {
        return nil
    }
    n, err := f.f.Write(f.wbuf[:f.n])
    if err != nil {
        if n > 0 && n < f.n {
            copy(f.wbuf[0:f.n-n], f.wbuf[n:f.n])
        }
        f.n -= n
        return err
    }
    f.n = 0
    return nil
}

func (f *FileWithBuffer) Size() int64 {
    return f.size
}

func (f *FileWithBuffer) Sync() error {
    f.Flush()
    return f.f.Sync()
}

func (f *FileWithBuffer) Close() error {
    f.Flush()
    return f.f.Close()
}

func (f *FileWithBuffer) Path() string {
    return f.path
}

