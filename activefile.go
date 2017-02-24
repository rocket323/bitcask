package bitcask

import (
    "bytes"
    "strconv"
    "encoding/binary"
)

type ActiveFile struct {
    id          int64
    f           *RandomAccessFile
}

func OpenActiveFile(id int64, create bool) (*ActiveFile, error) {
    idStr := strconv.Format(id, 10)
    raf, err := NewRandomAccessFile(idStr + ".data", create)
    if err != nil {
        log.Fatal(err)
        return err
    }

    af := &ActiveFile {
        id: id,
        f: raf
    }
    return af, nil
}

func (af *ActiveFile) Close() error {
    err := af.f.Close()
    if err != nil {
        log.Fatal(err)
        return err
    }
    return nil
}

func (af *ActiveFile) AddRecord(rec *Record) error {
    buf, err := rec.Encode()
    if err != nil {
        log.Fatal(err)
        return err
    }

    err = af.f.Append(buf)
    if err != nil {
        log.Fatal(err)
        return err
    }
    return nil
}

type Record struct {
    crc32       uint32
    timeStamp   uint32
    keySize     int64
    valueSize   int64
    key         []byte
    value       []byte
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

func (r *Record) Decode(buf []byte) error {
    b := bytes.NewBuffer(buf)
    var d = []interface{} {
        &r.crc32,
        &r.timeStamp,
        &r.keySize,
        &r.valueSize,
    }
    for _, v := range d {
        err := binary.Read(b, binary.LittleEndian, v)
        if err != nil {
            log.Fatal(err)
            return err
        }
    }
    r.key = make([]byte, r.keySize)
    err := binary.Read(b, binary.LittleEndian, r.key)
    if err != nil {
        log.Fatal(err)
        return err
    }
    r.value = make([]byte, r.valueSize)
    err := binary.Read(b, binary.LittleEndian, r.value)
    if err != nil {
        log.Fatal(err)
        return err
    }
    return nil
}

