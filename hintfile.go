package bitcask

import (
    "io"
    "bytes"
    "encoding/binary"
    "log"
    "crypto/md5"
)

type HintItem struct {
    flag            uint8
    expration       uint32
    valueSize       int64
    valuePos        int64
    keySize         int64
    key             []byte
}

const (
    HINT_FILE_HEADER_SIZE = 8 + md5.Size
    HINT_ITEM_HEADER_SIZE = 29
)

func (hi *HintItem) Encode() ([]byte, error) {
    buf := new(bytes.Buffer)
    var data = []interface{}{
        hi.flag,
        hi.expration,
        hi.valueSize,
        hi.valuePos,
        hi.keySize,
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
    header := make([]byte, HINT_ITEM_HEADER_SIZE)
    _, err := f.ReadAt(header, offset)
    if err != nil {
        return nil, err
    }

    hi := &HintItem{
        flag:           uint8(header[0]),
        expration:      uint32(binary.LittleEndian.Uint32(header[1:5])),
        valueSize:      int64(binary.LittleEndian.Uint64(header[5:13])),
        valuePos:       int64(binary.LittleEndian.Uint64(header[13:21])),
        keySize:        int64(binary.LittleEndian.Uint64(header[21:29])),
    }

    offset += HINT_ITEM_HEADER_SIZE
    hi.key = make([]byte, hi.keySize)
    _, err = f.ReadAt(hi.key, offset)
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

type FileMeta struct {
    FileId int64
    Md5 []byte
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

func (hf *HintFile) WriteHeader(md5sum []byte) error {
    if err := binary.Write(hf, binary.LittleEndian, hf.id); err != nil {
        return err
    }
    if err := binary.Write(hf, binary.LittleEndian, md5sum); err != nil {
        return err
    }
    return nil
}

func (hf *HintFile) ForEachItem(fn func(item *HintItem) error) error {
    var offset int64 = HINT_FILE_HEADER_SIZE
    for {
        hi, err := parseHintItemAt(hf, offset)
        if err != nil {
            if err == io.EOF {
                break
            }
            return err
        }

        err = fn(hi)
        if err != nil {
            return err
        }

        offset += HINT_ITEM_HEADER_SIZE + int64(hi.keySize)
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

