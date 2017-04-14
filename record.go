package bitcask

import (
    "log"
    "bytes"
    "encoding/binary"
    "io"
    "github.com/rocket323/bitcask/lru"
)

type Record struct {
    flag        uint16
    crc32       uint32
    expration   uint32
    keySize     int64
    valueSize   int64
    value       []byte
    key         []byte
}

const (
    RECORD_FLAG_DELETED = 1 << iota
)

const (
    RECORD_HEADER_SIZE = 25
)

func (r *Record) Size() int64 {
    return RECORD_HEADER_SIZE + r.keySize + r.valueSize
}

func RecordValueOffset() int64 {
    return RECORD_HEADER_SIZE
}

func (r *Record) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)
    var data = []interface{}{
        r.flag,
        r.crc32,
        r.expration,
        r.keySize,
        r.valueSize,
        r.value,
        r.key,
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

func parseRecordAt(f FileReader, offset int64) (*Record, error) {
    header, err := f.ReadAt(offset, RECORD_HEADER_SIZE)
    if err != nil {
        if err != io.EOF {
            log.Println(err)
        }
        return nil, err
    }

    rec := &Record{
        flag:           uint16(binary.LittleEndian.Uint16(header[0:2])),
        crc32:          uint32(binary.LittleEndian.Uint32(header[2:6])),
        expration:      uint32(binary.LittleEndian.Uint32(header[6:10])),
        keySize:        int64(binary.LittleEndian.Uint64(header[10:18])),
        valueSize:      int64(binary.LittleEndian.Uint64(header[18:26])),
    }

    offset += RECORD_HEADER_SIZE
    rec.value, err = f.ReadAt(offset, rec.valueSize)
    if err != nil {
        log.Println(err)
        return nil, err
    }

    offset += rec.keySize
    rec.key, err = f.ReadAt(offset, rec.keySize)
    if err != nil {
        log.Println(err)
        return nil, err
    }

    return rec, nil
}

/////////////////////////////////

type RecordIter struct {
    df          *DataFile
    recPos      int64
    rec         *Record
    valid       bool
    bc          *BitCask
}

func NewRecordIter(df *DataFile, bc *BitCask) *RecordIter {
    iter := &RecordIter {
        df: df,
        recPos: 0,
        rec: nil,
        valid: false,
        bc: bc,
    }
    return iter
}

func (it *RecordIter) Reset() {
    it.recPos = 0
    it.valid = true
    var err error
    it.rec, err = it.bc.recCache.Ref(it.df.id, it.recPos)
    if err != nil {
        it.valid = false
        return
    }
}

func (it *RecordIter) Close() {
    it.valid = false
    it.df.fr.Close()
}

func (it *RecordIter) Valid() bool {
    return it.valid
}

func (it *RecordIter) Next() {
    if !it.valid || it.rec == nil {
        return
    }
    it.recPos += it.rec.Size()
    var err error
    it.rec, err = it.bc.recCache.Ref(it.df.id, it.recPos)
    if err != nil {
        it.valid = false
        return
    }
}

func (it *RecordIter) Key() []byte {
    return it.rec.key
}

func (it *RecordIter) Value() []byte {
    return it.rec.value
}

/////////////////////////////////
type RecordCache struct {
    cache           *lru.Cache
    capacity        int
    bc              *BitCask
}

type RecordKey struct {
    fileId  int64
    pos     int64
}

func NewRecordCache(capacity int, bc *BitCask) *RecordCache {
    c := lru.NewCache(capacity, nil)
    rc := &RecordCache{
        cache: c,
        capacity: capacity,
        bc: bc,
    }
    return rc
}

func (rc *RecordCache) Ref(fileId int64, offset int64) (*Record, error) {
    recKey := RecordKey{fileId, offset}
    v, err := rc.cache.Ref(recKey)
    if err == nil {
        return v.(*Record), nil
    }

    // parse record at data file
    df, err := rc.bc.dfCache.Ref(fileId)
    if err != nil {
        return nil, err
    }
    defer rc.bc.dfCache.Unref(fileId)

    rec, err := parseRecordAt(df.fr, offset)
    if err != nil {
        return nil, err
    }
    rc.cache.Put(recKey, rec)
    rc.cache.Ref(recKey)

    return rec, nil
}

func (rc *RecordCache) Unref(fileId int64, offset int64) {
    recKey := RecordKey{fileId, offset}
    rc.cache.Unref(recKey)
}

func (rc *RecordCache) Close() {
    rc.cache.Close()
}

