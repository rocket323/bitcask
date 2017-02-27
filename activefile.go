package bitcask

import (
    "log"
)

type ActiveFile struct {
    *RandomAccessFile
}

func (af *ActiveFile) AddRecord(rec *Record) error {
    buf, err := rec.Encode()
    if err != nil {
        log.Fatal(err)
        return err
    }

    err = af.Append(buf)
    if err != nil {
        log.Fatal(err)
        return err
    }
    return nil
}

