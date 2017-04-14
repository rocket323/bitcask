package bitcask

import (
    "io"
    "bytes"
    "encoding/binary"
    "log"
)

type HintItem struct {
    expration       uint32
    keySize         int64
    valueSize       int64
    valuePos        int64
    key             []byte
}

const (
    HINT_ITEM_HEADER_SIZE = 28
)

func (hi *HintItem) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)
    var data = []interface{}{
        hi.expration,
        hi.keySize,
        hi.valueSize,
        hi.valuePos,
        hi.key,
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

func parseHintItemAt(f FileReader, offset int64) (*HintItem, error) {
    header, err := f.ReadAt(offset, HINT_ITEM_HEADER_SIZE)
    if err != nil {
        return nil, err
    }

    hi := &HintItem{
        expration:      uint32(binary.LittleEndian.Uint32(header[0:4])),
        keySize:        int64(binary.LittleEndian.Uint64(header[4:12])),
        valueSize:      int64(binary.LittleEndian.Uint64(header[12:20])),
        valuePos:       int64(binary.LittleEndian.Uint64(header[20:28])),
    }

    offset += HINT_ITEM_HEADER_SIZE
    hi.key, err = f.ReadAt(offset, hi.keySize)
    if err != nil {
        log.Println(err)
        return nil, err
    }

    return hi, nil
}

type HintFile struct {
    *FileWithBuffer
    id int64
}

func NewHintFile(path string, id int64, wbufSize int64) (*HintFile, error) {
    f, err := NewFileWithBuffer(path, true, wbufSize)
    if err != nil {
        return nil, err
    }

    hf := &HintFile{
        FileWithBuffer: f,
        id: id,
    }
    return hf, nil
}

func (hf *HintFile) ForEachItem(fn func(item *HintItem) error) error {
    var offset int64 = 0
    for {
        hi, err := parseHintItemAt(hf, offset)
        if err != nil {
            if err == io.EOF {
                break
            }
            log.Println(err)
            return err
        }

        err = fn(hi)
        if err != nil {
            log.Println(err)
            return err
        }

        offset += HINT_ITEM_HEADER_SIZE + hi.keySize
    }
    return nil
}

func (hf *HintFile) AddItem(item *HintItem) error {
    buf, err := item.Encode()
    if err != nil {
        log.Println(err)
        return err
    }

    _, err = hf.Write(buf)
    if err != nil {
        log.Println(err)
        return err
    }
    return nil
}

