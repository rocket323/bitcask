package bitcask

import (
    "log"
    "bytes"
    "encoding/binary"
    "io"
)

type Record struct {
    crc32       uint32
    timeStamp   uint32
    keySize     int64
    valueSize   int64
    key         []byte
    value       []byte
}

const (
    RECORD_HEADER_SIZE = 24
)

func (r *Record) Size() int64 {
    return RECORD_HEADER_SIZE + r.keySize + r.valueSize
}

func (r *Record) ValueFieldOffset() int64 {
    return RECORD_HEADER_SIZE + r.keySize
}

func (r *Record) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)
    var data = []interface{} {
        r.crc32,
        r.timeStamp,
        r.keySize,
        r.valueSize,
        r.key,
        r.value,
    }
    for _, v := range data {
        err := binary.Write(buf, binary.LittleEndian, v)
        if err != nil {
            log.Println(err)
            return nil, err
        }
    }
    return buf.Bytes(), nil
}

func ParseRecordAt(raf *RandomAccessFile, offset int64) (*Record, error) {
    header, err := raf.ReadAt(offset, RECORD_HEADER_SIZE)
    if err != nil {
        if err != io.EOF {
            log.Println(err)
        }
        return nil, err
    }

    rec := &Record {
        crc32:          uint32(binary.LittleEndian.Uint32(header[0:4])),
        timeStamp:      uint32(binary.LittleEndian.Uint32(header[4:8])),
        keySize:        int64(binary.LittleEndian.Uint64(header[8:16])),
        valueSize:      int64(binary.LittleEndian.Uint64(header[16:24])),
    }

    offset += RECORD_HEADER_SIZE
    rec.key, err = raf.ReadAt(offset, rec.keySize)
    if err != nil {
        log.Println(err)
        return nil, err
    }

    offset += rec.keySize
    rec.value, err = raf.ReadAt(offset, rec.valueSize)
    if err != nil {
        log.Println(err)
        return nil, err
    }

    return rec, nil
}

type RecordIter struct {
    f           *RandomAccessFile
    curPos      int64
    curRec      *Record
    valid       bool
}

func NewRecordIter(f *RandomAccessFile) *RecordIter {
    iter := &RecordIter {
        f: f,
        curPos: 0,
        curRec: nil,
        valid: false,
    }
    return iter
}

func (it *RecordIter) Reset() {
    err := it.f.Seek(0)
    if err != nil {
        return
    }
    it.curPos = 0
    it.valid = true
    it.curRec, err = ParseRecordAt(it.f, it.curPos)
    if err != nil {
        it.valid = false
        return
    }
}

func (it *RecordIter) Close() {
    it.valid = false
    it.f.Close()
}

func (it *RecordIter) Valid() bool {
    return it.valid
}

func (it *RecordIter) Next() {
    if !it.valid || it.curRec == nil {
        return
    }
    it.curPos += it.curRec.Size()
    var err error
    it.curRec, err = ParseRecordAt(it.f, it.curPos)
    if err != nil {
        it.valid = false
        return
    }
}

func (it *RecordIter) Key() []byte {
    return it.curRec.key
}

func (it *RecordIter) Value() []byte {
    return it.curRec.value
}

