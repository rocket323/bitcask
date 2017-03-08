package bitcask

import (
    "log"
)

type ActiveFile struct {
    *FileWithBuffer
    id  int64
}

func NewActiveFile(path string, id int64, wbufSize int64) (*ActiveFile, error) {
    f, err := NewFileWithBuffer(path, true, wbufSize)
    if err != nil {
        return nil, err
    }

    af := &ActiveFile{
        FileWithBuffer: f,
        id: id,
    }
    return af
}

func (af *ActiveFile) AddRecord(rec *Record) error {
    buf, err := rec.Encode()
    if err != nil {
        log.Println(err)
        return err
    }

    err = af.Write(buf)
    if err != nil {
        log.Println(err)
        return err
    }
    return nil
}

