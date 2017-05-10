package bitcask

import (
    "hash/crc32"
    "log"
    "bytes"
    "encoding/binary"
    "io"
    "github.com/rocket323/bitcask/lru"
)

type Record struct {
    crc32       uint32
    flag        uint8
    expration   uint32
    valueSize   int64
    keySize     int64
    value       []byte
    key         []byte
}

const (
    RECORD_FLAG_DELETED = 1 << iota
    RECORD_FLAG_BATCH
    RECORD_FLAG_MERGE       // record for merge info, i.e. delete file
)

const (
    RECORD_HEADER_SIZE = 25
)

func (r *Record) Size() int64 {
    return RECORD_HEADER_SIZE + int64(r.keySize + r.valueSize)
}

func RecordValueOffset() int64 {
    return RECORD_HEADER_SIZE
}

func (r *Record) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)
    var data = []interface{}{
        r.flag,
        r.expration,
        r.valueSize,
        r.keySize,
        r.value,        // len(value) can be zero
        r.key,
    }

    for _, v := range data {
        err := binary.Write(buf, binary.LittleEndian, v)
        if err != nil {
            log.Println(err)
            return nil, err
        }
    }

    // calc crc32 and append it to head
    r.crc32 = crc32.ChecksumIEEE(buf.Bytes())
    crc := make([]byte, 4)
    binary.LittleEndian.PutUint32(crc, r.crc32)

    return append(crc, buf.Bytes()...), nil
}

func parseRecordAt(r io.ReaderAt, offset int64) (*Record, error) {
    header := make([]byte, RECORD_HEADER_SIZE)
    _, err := r.ReadAt(header, offset)
    if err != nil {
        if err != io.EOF {
            log.Println(err)
        }
        return nil, err
    }

    rec := &Record{
        crc32:          uint32(binary.LittleEndian.Uint32(header[0:4])),
        flag:           header[4],
        expration:      uint32(binary.LittleEndian.Uint32(header[5:9])),
        valueSize:      int64(binary.LittleEndian.Uint64(header[9:17])),
        keySize:        int64(binary.LittleEndian.Uint64(header[17:25])),
    }
    crc := crc32.ChecksumIEEE(header[4:])

    if rec.flag & RECORD_FLAG_MERGE == 0 {
        offset += RECORD_HEADER_SIZE
        rec.value = make([]byte, rec.valueSize)
        _, err = r.ReadAt(rec.value, offset)
        if err != nil {
            log.Println(err)
            return nil, err
        }

        offset += rec.valueSize
        rec.key = make([]byte, rec.keySize)
        _, err = r.ReadAt(rec.key, offset)
        if err != nil {
            log.Println(err)
            return nil, err
        }

        crc = crc32.Update(crc, crc32.IEEETable, rec.value)
        crc = crc32.Update(crc, crc32.IEEETable, rec.key)
    }

    // check crc
    if crc != rec.crc32 {
        return nil, ErrRecordCorrupted
    }

    return rec, nil
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

