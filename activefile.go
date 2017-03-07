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

