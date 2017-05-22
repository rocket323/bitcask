package bitcask

import (
    "hash/crc32"
    "crypto/md5"
    "io"
    "bytes"
    "fmt"
    "sync"
    "io/ioutil"
    "log"
    "time"
    "os"
)

var (
    ErrKeyNotFound = fmt.Errorf("key not found")
    ErrRecordCorrupted = fmt.Errorf("record corrupted")
    ErrInvalid = fmt.Errorf("invalid")
)

type BitCask struct {
    mu              *sync.RWMutex
    dir             string
    opts            *Options
    activeFile      *ActiveFile
    activeKD        *KeyDir
    keyDir          *KeyDir
    recCache        *RecordCache
    dfCache         *DataFileCache
    isMerging       int32
    minDataFileId   int64
    maxDataFileId   int64

    // slots info
    keysInSlot      map[uint32]map[string]bool
    keysInTag       map[string]map[string]bool

    // file metas: fileId, md5, etc.
    fileMetas       []*FileMeta
}

func (bc *BitCask) clear() {
    bc.activeFile = nil
    bc.activeKD = NewKeyDir()
    bc.keyDir = NewKeyDir()
    bc.isMerging = 0
    bc.minDataFileId = 0
    bc.maxDataFileId = 0
    bc.keysInSlot = make(map[uint32]map[string]bool)
    bc.keysInTag = make(map[string]map[string]bool)
    bc.fileMetas = make([]*FileMeta, 0)
    bc.recCache = NewRecordCache(bc)
    bc.dfCache = NewDataFileCache(bc)
}

func Open(dir string, opts *Options) (*BitCask, error) {
    bc := &BitCask{
        mu: &sync.RWMutex{},
        dir: dir,
        opts: opts,
    }
    bc.clear()
    log.Printf("open at %s", dir)

    err := bc.Restore(-1)
    if err != nil {
        log.Printf("restore failed, err = %s", err)
        return nil, err
    }
    log.Printf("open succ.")
    return bc, nil
}

func (bc *BitCask) Restore(fileIdRange int64) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    begin := time.Now()
    files, err := ioutil.ReadDir(bc.dir)
    if err != nil {
        log.Println(err)
        return err
    }

    var corrupted bool = false
    for _, file := range files {
        name := file.Name()
        var err error
        var id int64
        id, err = getIdFromDataPath(name)
        if err != nil {
            continue
        }
        var outOfRange bool = false
        if fileIdRange >= 0 && id >= fileIdRange {
            outOfRange = true
        }

        if corrupted || outOfRange {
            err := bc.removeDataFile(id)
            if err != nil {
                log.Fatalf("remove data-file[%d] failed, err = %s", id, err)
                return err
            }
            continue
        }

        dataPath := bc.GetDataFilePath(id)
        hintPath := bc.getHintFilePath(id)

        var kd *KeyDir
        if _, err = os.Stat(hintPath); err == nil {
            kd, err = bc.restoreFromHintFile(hintPath, id)
        } else {
            kd, err = bc.restoreFromDataFile(dataPath, id)
        }
        if err != nil {
            log.Printf("data-file[%d], corrupted! remove it.", id)
            err := bc.removeDataFile(id)
            if err != nil {
                log.Fatalf("remove data-file[%d] failed, err = %s", id, err)
                return err
            }
            corrupted = true
            continue
        }

        md5, err := bc.getDataFileMd5(id)
        if err != nil {
            log.Fatalf("calc md5 for data-file[%d] failed, err = %s", id, err)
            return err
        }
        bc.addFileMeta(id, md5)

        if id < bc.minDataFileId {
            bc.minDataFileId = id
        }
        if id > bc.maxDataFileId {
            bc.maxDataFileId = id
            bc.activeKD = kd
        }
    }

    // make active file
    bc.activeFile, err = NewActiveFile(bc.GetDataFilePath(bc.maxDataFileId), bc.maxDataFileId, bc.opts.bufferSize)
    if err != nil {
        return err
    }

    // remove active file meta
    if len(bc.fileMetas) > 0 {
        bc.fileMetas = bc.fileMetas[:len(bc.fileMetas) - 1]
    }

    end := time.Now()
    log.Printf("restore succ! costs %.2f seconds.", end.Sub(begin).Seconds())
    return nil
}

func (bc *BitCask) updateKeyDir(key []byte, di *DirItem, akd *KeyDir, fillSlot bool) error {
    old, err := bc.keyDir.Get(key)
    // add to keydir
    if (err == nil && di.fileId >= old.fileId) || err == ErrKeyNotFound {
        if err := bc.keyDir.Put(key, di); err != nil {
            return err
        }
    }
    if err != ErrKeyNotFound {
        return err
    }
    // add to active keydir
    if akd != nil {
        if err := akd.Put(key, di); err != nil {
            return err
        }
    }

    // fill slot
    if fillSlot {
        tag, slot := HashKeyToSlot(key)
        if bc.keysInSlot[slot] == nil {
            bc.keysInSlot[slot] = make(map[string]bool)
        }
        bc.keysInSlot[slot][string(key)] = true

        if len(tag) < len(key) {
            if bc.keysInTag[string(tag)] == nil {
                bc.keysInTag[string(tag)] = make(map[string]bool)
            }
            bc.keysInTag[string(tag)][string(key)] = true
        }
    }

    return nil
}

func (bc *BitCask) restoreFromHintFile(path string, id int64) (*KeyDir, error) {
    log.Printf("restore data from hint-file[%d]", id)
    hf, err := NewHintFile(path, id, bc.opts.bufferSize)
    if err != nil {
        return nil, err
    }

    activeKD := NewKeyDir()
    err = hf.ForEachItem(func (item *HintItem) error {
        di := &DirItem{
            flag: item.flag,
            fileId: hf.id,
            valuePos: item.valuePos,
            valueSize: item.valueSize,
            expration: item.expration,
        }
        if err := bc.updateKeyDir(item.key, di, activeKD, true); err != nil {
            return err
        }
        return nil
    })

    if err != nil {
        return nil, err
    }
    return activeKD, nil
}

func (bc *BitCask) restoreFromDataFile(path string, id int64) (*KeyDir, error) {
    log.Printf("restore data from data-file[%d]", id)
    df, err := NewDataFile(path, id)
    if err != nil {
        return nil, err
    }

    activeKD := NewKeyDir()
    err = df.ForEachItem(func (rec *Record, offset int64) error {
        di := &DirItem{
            flag: rec.flag,
            fileId: df.id,
            valuePos: offset + RecordValueOffset(),
            valueSize: rec.valueSize,
            expration: rec.expration,
        }
        if err := bc.updateKeyDir(rec.key, di, activeKD, true); err != nil {
            return err
        }
        return nil
    })
    if err != nil {
        return nil, err
    }
    return activeKD, nil
}

func (bc *BitCask) Get(key []byte) ([]byte, error) {
    value, _, err := bc.GetWithExpr(key)
    return value, err
}

func (bc *BitCask) GetWithExpr(key []byte) ([]byte, uint32, error) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    di, err := bc.keyDir.Get(key)
    if err != nil {
        return nil, 0, err
    }

    if di.flag & RECORD_FLAG_DELETED > 0 {
        log.Printf("key[%s] has been deleted", string(key))
        return nil, 0, ErrKeyNotFound
    }

    rec, err := bc.refRecord(di.fileId, int64(di.valuePos) - RecordValueOffset())
    if err != nil {
        log.Printf("ref file[%d] at offset[%d] failed, err=%s\n", di.fileId, int64(di.valuePos) - RecordValueOffset(), err)
        return nil, 0, err
    }
    defer bc.unrefRecord(di.fileId, int64(di.valuePos) - RecordValueOffset())
    return rec.value, di.expration, nil
}

func (bc *BitCask) Del(key []byte) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    rec := &Record{
        flag: RECORD_FLAG_DELETED,
        keySize: int64(len(key)),
        key: make([]byte, len(key)),
    }
    copy(rec.key, key)
    return bc.addRecord(rec, true)
}

func (bc *BitCask) DelLocal(key []byte) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    rec := &Record{
        flag: RECORD_FLAG_DELETED,
        keySize: int64(len(key)),
        key: make([]byte, len(key)),
    }
    copy(rec.key, key)
    return bc.addRecord(rec, false)
}

func (bc *BitCask) Set(key []byte, val []byte) error {
    return bc.SetWithExpr(key, val, 0)
}

func (bc *BitCask) SetWithExpr(key []byte, value []byte, expration uint32) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    keySize := len(key)
    valueSize := len(value)
    rec := &Record{
        expration: expration,
        valueSize: int64(valueSize),
        keySize: int64(keySize),
        value: make([]byte, valueSize),
        key: make([]byte, keySize),
    }
    copy(rec.key, key)
    copy(rec.value, value)
    return bc.addRecord(rec, true)
}

const (
    MaxSlotNum = 1024
)

func HashTag(key []byte) []byte {
    part := key
    if i := bytes.IndexByte(part, '{'); i != -1 {
        part = part[i+1:]
    } else {
        return key
    }
    if i := bytes.IndexByte(part, '}'); i != -1 {
        return part[:i]
    } else {
        return key
    }
}

func HashTagToSlot(tag []byte) uint32 {
    return crc32.ChecksumIEEE(tag) % MaxSlotNum
}

func HashKeyToSlot(key []byte) ([]byte, uint32) {
    tag := HashTag(key)
    return tag, HashTagToSlot(tag)
}

func (bc *BitCask) AddRecord(rec *Record, fillSlot bool) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()
    return bc.addRecord(rec, fillSlot)
}

// requires bc.mu held
func (bc *BitCask) addRecord(rec *Record, fillSlot bool) error {
    offset := bc.activeFile.Size()
    err := bc.activeFile.AddRecord(rec)
    if err != nil {
        return err
    }

    if rec.flag & RECORD_FLAG_MERGE == 0 {
        di := &DirItem{
            flag: rec.flag,
            fileId: bc.activeFile.id,
            valuePos: offset + RecordValueOffset(),
            valueSize: rec.valueSize,
            expration: rec.expration,
        }

        if err := bc.updateKeyDir(rec.key, di, bc.activeKD, fillSlot); err != nil {
            return err
        }
    }

    if bc.activeFile.Size() >= bc.opts.maxFileSize {
        bc.rotateActiveFile(bc.activeFile.id + 1)
    }
    return nil
}

func (bc *BitCask) rotateActiveFile(nextFileId int64) error {
    log.Printf("rotate activeFile to %d", nextFileId)
    bc.activeFile.Close()

    err := bc.generateHintFile(bc.activeFile.id)
    if err != nil {
        log.Println(err)
        return err
    }
    bc.activeKD.Clear()

    af, err := NewActiveFile(bc.GetDataFilePath(nextFileId), nextFileId, bc.opts.bufferSize)
    if err != nil {
        return err
    }
    bc.activeFile = af
    bc.maxDataFileId = nextFileId

    return nil
}

func (bc *BitCask) getDataFileMd5(fileId int64) ([]byte, error) {
    path := bc.GetDataFilePath(fileId)
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()
    h := md5.New()
    if _, err = io.Copy(h, f); err != nil {
        return nil, err
    }
    md5sum := h.Sum(nil)
    return md5sum[:], nil
}

func (bc *BitCask) addFileMeta(fileId int64, md5 []byte) {
    meta := &FileMeta{
        FileId: fileId,
        Md5: md5,
    }
    bc.fileMetas = append(bc.fileMetas, meta)
}

func (bc *BitCask) generateHintFile(fileId int64) error {
    hf, err := NewHintFile(bc.getHintFilePath(fileId), fileId, bc.opts.bufferSize)
    if err != nil {
        return err
    }
    defer hf.Close()

    // write md5sum to hint file header
    md5, err := bc.getDataFileMd5(fileId)
    if err != nil {
        return err
    }
    bc.addFileMeta(fileId, md5)

    if err := hf.WriteHeader(md5); err != nil {
        return err
    }

    for key, di := range bc.activeKD.mp {
        hi := &HintItem{
            flag: di.flag,
            expration: di.expration,
            valueSize: di.valueSize,
            valuePos: di.valuePos,
            keySize: int64(len(key)),
            key: []byte(key),
        }
        err := hf.AddItem(hi)
        if err != nil {
            return err
        }
    }

    return nil
}

func (bc *BitCask) GetFileMetas() []*FileMeta {
    return bc.fileMetas
}

func (bc *BitCask) Close() error {
    bc.mu.Lock()
    defer bc.mu.Unlock()
    return bc.close()
}

func (bc *BitCask) close() error {
    bc.activeFile.Close()
    if bc.recCache != nil {
        bc.recCache.Close()
    }
    if bc.dfCache != nil {
        bc.dfCache.Close()
    }
    return nil
}

func (bc *BitCask) ClearAll() error {
    log.Printf("clearing db[%s]...", bc.dir)
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.close()
    bc.clear()
    if err := os.RemoveAll(bc.dir); err != nil {
        log.Fatalf("remove dir[%s] failed, err = %s", bc.dir, err)
    }
    if err := os.Mkdir(bc.dir, 0755); err != nil {
        log.Fatalf("recreate dir[%s] failed, err = %s", bc.dir, err)
    }

    // make active file
    var err error
    bc.activeFile, err = NewActiveFile(bc.GetDataFilePath(bc.maxDataFileId), bc.maxDataFileId, bc.opts.bufferSize)
    if err != nil {
        log.Fatal(err)
        return err
    }

    log.Printf("clear db[%s] succ!", bc.dir)
    return nil
}

// truncate bitcask database to [0, fileId)
func (bc *BitCask) Truncate(fileId int64) error {
    log.Printf("truncate db[%s] to [0, %d)", bc.dir, fileId)
    bc.mu.Lock()
    bc.mu.Unlock()

    bc.close()
    bc.clear()

    err := bc.Restore(fileId)
    if err != nil {
        log.Fatalf("truncate db[%s] to [0, %d) failed, err = %s", bc.dir, fileId, err)
        return err
    }
    return nil
}

func (bc *BitCask) removeDataFile(fileId int64) error {
    dataPath := bc.GetDataFilePath(fileId)
    hintPath := bc.getHintFilePath(fileId)
    if _, err := os.Stat(dataPath); err == nil {
        if err := os.Remove(dataPath); err != nil {
            return err
        }
    }
    if _, err := os.Stat(hintPath); err == nil {
        if err := os.Remove(hintPath); err != nil {
            return err
        }
    }
    return nil
}

func (bc *BitCask) SyncFile(fileId int64, offset int64, length int64, data []byte) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    if fileId > bc.activeFile.id {
        bc.rotateActiveFile(fileId)
    }
    af := bc.activeFile

    if fileId != af.id {
        log.Printf("invalid sync, active fileId[%d] != sync fildId[%d]", af.id, fileId)
        return ErrInvalid
    }

    if af.Size() != offset {
        log.Printf("invalid sync, active file[%d], size[%d] != sync offset[%d]", af.id, af.Size(), offset)
        return ErrInvalid
    }

    rec, err := parseRecordAt(bytes.NewReader(data), 0)
    if err != nil {
        log.Printf("parse record failed, err = %s", err)
        return err
    }

    // if it's a merge record
    if rec.flag & RECORD_FLAG_MERGE > 0 {
        f := rec.valueSize
        if err := bc.removeDataFile(f); err != nil {
            log.Printf("remove merged file[%d] failed, err = %s", fileId, err)
            return err
        }
    }

    err = bc.addRecord(rec, true)
    if err != nil {
        log.Printf("append record failed, err = %s", err)
        return err
    }

    return nil
}

func (bc *BitCask) FirstKeyUnderSlot(slot uint32) ([]byte, error) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    keys := bc.keysInSlot[slot]
    if keys == nil {
        return nil, nil
    }
    // return a random key
    for k, _ := range keys {
        delete(keys, k)
        return []byte(k), nil
    }
    return nil, nil
}

func (bc *BitCask) AllKeysWithTag(tag []byte) ([][]byte, error) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    results := make([][]byte, 0)
    keys, ok := bc.keysInTag[string(tag)]
    if !ok || keys == nil {
        results = append(results, tag)
    } else {
        for k, _ := range keys {
            results = append(results, []byte(k))
        }
    }
    delete(bc.keysInTag, string(tag))
    return results, nil
}

