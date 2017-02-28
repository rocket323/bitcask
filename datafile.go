package bitcask

import (
    "log"
    "fmt"
    "strconv"
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
    err := it.f.Seek(0)
    if err != nil {
        log.Println(err)
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

func GetFileId(name string) (int64, error) {
    id, err := strconv.ParseInt(name, 10, 64)
    if err != nil {
        return -1, err
    }
    return id, nil
}

func GetFileBaseName(id int64) string {
    if id < 0 {
        log.Println("fileId[%d] < 0", id)
        return "invalid"
    }
    return fmt.Sprintf("%09d", id)
}


