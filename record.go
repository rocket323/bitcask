package bitcask

type Record struct {
    crc32       uint32
    timeStamp   uint32
    keySize     int64
    valueSize   int64
    key         []byte
    value       []byte
}

const (
    RECORD_HEADER_SIZE = 192
)

func (r *Record) Size() int64 {
    return RECORD_HEADER_SIZE + keySize + valueSize
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
            log.Fatal(err)
            return err
        }
    }
    return buf.Bytes(), nil
}

func ParseRecordAt(raf *RandomAccessFile, offset int64) (*Record, error) {
    header, err := raf.ReadAt(offset, RECORD_HEADER_SIZE)
    if err != nil {
        log.Fatal(err)
        return err
    }

    rec := &Record {
        crc32:          binary.LittleEndian.Uint32(header[0:4]),
        timeStamp:      binary.LittleEndian.Uint32(header[4:8]),
        keySize:        binary.LittleEndian.Uint64(header[8:16]),
        valueSize:      binary.LittleEndian.Uint64(header[12:18]),
    }

    offset += RECORD_HEADER_SIZE
    rec.key, err = raf.ReadAt(offset, rec.keySize)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    offset += keySize
    rec.value, err = raf.ReadAt(offset, rec.valueSize)
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    return rec, nil
}


