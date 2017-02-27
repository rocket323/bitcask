package bitcask

import (
    "log"
    "os"
)

type DataIter struct {
    f           *RandomAccessFile
    offset      int64
    valid       bool
    rec         *Record
}

func NewDataIter(f *RandomAccessFile) (*DataIter, error) {
    iter := &DataIter {
        f: f,
        offset: 0,
        valid: true,
        rec: nil,
    }
    return iter, nil
}

func (it *DataIter) Reset() {
    err := it.f.Seek(0, os.SEEK_SET)
    if err != nil {
        log.Fatal(err)
    }

    it.offset = 0

    rec, err := ParseRecordAt(it.f, it.offset)
    if err != nil {
        it.valid = false
        return
    }
    it.rec = rec
    it.valid = true
}

func (it *DataIter) Valid() bool {
    return it.valid
}

func (it *DataIter) Next() {
    if !it.valid {
        return
    }
    it.offset += it.rec.Size()
    rec, err := ParseRecordAt(it.f, it.offset)
    if err != nil {
        it.valid = false
        return
    }
    it.rec = rec
}

