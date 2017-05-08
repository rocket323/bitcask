package bitcask

import (
    "log"
    "bytes"
    "encoding/binary"
    "io"
    "github.com/rocket323/bitcask/lru"
)

type Record struct {
    slot        uint16
    flag        uint16
    crc32       uint32
    expration   uint32
    valueSize   int64
    keySize     int64
    value       []byte
    key         []byte
}

const (
    RECORD_FLAG_DELETED = 1 << iota
)

const (
    RECORD_HEADER_SIZE = 28
)

func (r *Record) Size() int64 {
    var valueSize int64 = 0
    if r.valueSize > 0 {
        valueSize = r.valueSize
    }
    return RECORD_HEADER_SIZE + r.keySize + valueSize
}

func RecordValueOffset() int64 {
    return RECORD_HEADER_SIZE
}

func (r *Record) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)
    var data = []interface{}{
        r.slot,
        r.flag,
        r.crc32,
        r.expration,
        r.valueSize,
        r.keySize,
    }
    if r.valueSize > 0 {
        data = append(data, r.value)
    }
    data = append(data, r.key)

    for _, v := range data {
        err := binary.Write(buf, binary.LittleEndian, v)
        if err != nil {
            log.Println(err)
            return nil, err
        }
    }
    return buf.Bytes(), nil
}

func parseRecord(data []byte) (*Record, error) {
    rec := &Record{
        slot:           uint16(binary.LittleEndian.Uint16(data[0:2])),
        flag:           uint16(binary.LittleEndian.Uint16(data[2:4])),
        crc32:          uint32(binary.LittleEndian.Uint32(data[4:8])),
        expration:      uint32(binary.LittleEndian.Uint32(data[8:12])),
        valueSize:      int64(binary.LittleEndian.Uint64(data[12:20])),
        keySize:        int64(binary.LittleEndian.Uint64(data[20:28])),
    }
    var valueSize int64 = 0
    if (rec.valueSize > 0) {
        valueSize = rec.valueSize
    }

    if len(data) != int(rec.Size()) {
        log.Printf("data size[%d] != record size[%d]", len(data), rec.Size())
        return nil, ErrInvalid
    }

    var offset int64 = RECORD_HEADER_SIZE
    if valueSize >= 0 {
        rec.value = make([]byte, valueSize)
        _ = copy(rec.value, data[offset:])
    }

    offset += valueSize
    rec.key = make([]byte, rec.keySize)
    _ = copy(rec.key, data[offset:])
    return rec, nil
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
        slot:           uint16(binary.LittleEndian.Uint16(header[0:2])),
        flag:           uint16(binary.LittleEndian.Uint16(header[2:4])),
        crc32:          uint32(binary.LittleEndian.Uint32(header[4:8])),
        expration:      uint32(binary.LittleEndian.Uint32(header[8:12])),
        valueSize:      int64(binary.LittleEndian.Uint64(header[12:20])),
        keySize:        int64(binary.LittleEndian.Uint64(header[20:28])),
    }
    var valueSize int64 = 0
    if (rec.valueSize > 0) {
        valueSize = rec.valueSize
    }

    offset += RECORD_HEADER_SIZE
    if valueSize >= 0 {
        rec.value, err = f.ReadAt(offset, valueSize)
        if err != nil {
            log.Println(err)
            return nil, err
        }
    }

    offset += valueSize
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
}

func NewRecordIter(df *DataFile) *RecordIter {
    iter := &RecordIter {
        df: df,
        recPos: 0,
        rec: nil,
        valid: false,
    }
    return iter
}

func (it *RecordIter) Reset() {
    it.recPos = 0
    it.valid = true
    var err error
    it.rec, err = parseRecordAt(it.df, it.recPos)
    if err != nil {
        it.valid = false
        return
    }
}

func (it *RecordIter) Close() {
    it.valid = false
    it.df.Close()
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
    it.rec, err = parseRecordAt(it.df, it.recPos)
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
    env             Env
}

type RecordKey struct {
    fileId  int64
    pos     int64
}

func NewRecordCache(env Env) *RecordCache {
    opts := env.getOptions()
    c := lru.NewCache(int(opts.cacheSize), nil)

    rc := &RecordCache{
        cache: c,
        capacity: int(opts.cacheSize),
        env: env,
    }
    return rc
}

func (rc *RecordCache) Ref(fileId int64, offset int64) (*Record, error) {
    recKey := RecordKey{fileId, offset}
    v, err := rc.cache.Ref(recKey)
    if err == nil {
        return v.(*Record), nil
    }
    var fr FileReader
    env := rc.env

    if env.getActiveFile() == nil {
        log.Fatal("activeFiel is nil")
    }
    if fileId == env.getActiveFile().id {
        fr = env.getActiveFile()
    } else {
        df, err := env.refDataFile(fileId)
        if err != nil {
            return nil, err
        }
        defer env.unrefDataFile(fileId)
        fr = df
    }

    rec, err := parseRecordAt(fr, offset)
    if err != nil {
        log.Printf("fileId %d, offset: %d, size: %d, err = %s", fileId, offset, fr.Size(), err)
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

