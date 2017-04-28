package bitcask

import (
    "io"
    "fmt"
    "sync"
    "io/ioutil"
    "log"
    "time"
    "os"
    "path/filepath"
    "sync/atomic"
)

var (
    ErrNotFound = fmt.Errorf("not found")
    ErrNotCompl = fmt.Errorf("record not completed")
    ErrInvalid = fmt.Errorf("invalid")
)

type BitCask struct {
    mu          *sync.RWMutex
    dir         string
    keyDir      *KeyDir
    activeFile  *ActiveFile
    activeKD    *KeyDir
    opts        *Options
    version     uint64
    snaps       map[uint64]*Snapshot
    recCache    *RecordCache
    dfCache     *DataFileCache
    isMerging   int32
    minDataFileId int64
    maxDataFileId int64
}

func Open(dir string, opts *Options) (*BitCask, error) {
    bc := &BitCask{
        dir: dir,
        activeFile: nil,
        keyDir: NewKeyDir(),
        activeKD: NewKeyDir(),
        mu: &sync.RWMutex{},
        opts: opts,
        version: 0,
        snaps: make(map[uint64]*Snapshot),
        isMerging: 0,
        minDataFileId: -1,
        maxDataFileId: 0,
    }
    bc.recCache = NewRecordCache(bc)
    bc.dfCache = NewDataFileCache(bc)

    err := bc.Restore()
    if err != nil {
        log.Println(err)
        return nil, err
    }
    return bc, nil
}

func (bc *BitCask) Restore() error {
    begin := time.Now()
    files, err := ioutil.ReadDir(bc.dir)
    if err != nil {
        log.Println(err)
        return err
    }

    lastId := int64(0)
    for _, file := range files {
        name := file.Name()
        var err error
        var id int64
        id, err = getIdFromDataPath(name)
        if err != nil {
            // log.Printf("invalid datafile name[%s], skip\n", name)
            continue
        }

        dataPath := bc.GetDataFilePath(id)
        hintPath := bc.getHintFilePath(id)

        var kd *KeyDir
        // if hintfile exists, restore from hintfile
        if _, err = os.Stat(hintPath); err == nil {
            kd, err = bc.restoreFromHintFile(hintPath, id)
        } else { // otherwise, restore from datafile
            kd, err = bc.restoreFromDataFile(dataPath, id)
        }
        if err != nil {
            log.Fatal(err)
        }

        if id > lastId {
            bc.activeKD = kd
            lastId = id
        }
        if bc.minDataFileId == -1 || id < bc.minDataFileId {
            bc.minDataFileId = id
        }
        if id > bc.maxDataFileId {
            bc.maxDataFileId = id
        }
    }

    // make active file
    bc.activeFile, err = NewActiveFile(bc.GetDataFilePath(lastId), lastId, bc.opts.bufferSize)
    if err != nil {
        log.Println(err)
        return err
    }

    end := time.Now()
    log.Printf("bitcask restore succ! costs %.2f seconds.", end.Sub(begin).Seconds())

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
        iter_di := &DirItem{
            fileId: hf.id,
            valuePos: item.valuePos,
            valueSize: item.valueSize,
            expration: item.expration,
        }

        di, err := bc.keyDir.Get(string(item.key))
        if (err == nil && hf.id >= di.fileId) || err == ErrNotFound {
            // add to keydir
            err := bc.keyDir.Put(string(item.key), iter_di)
            if err != nil {
                return err
            }
        }

        // add to active keydir
        err = activeKD.Put(string(item.key), iter_di)
        if err != nil {
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
    iter := NewRecordIter(df)
    activeKD := NewKeyDir()
    for iter.Reset(); iter.Valid(); iter.Next() {
        rec := iter.rec
        iter_di := &DirItem{
            fileId: iter.df.id,
            valuePos: iter.recPos + RecordValueOffset(),
            valueSize: rec.valueSize,
            expration: rec.expration,
        }
        di, err := bc.keyDir.Get(string(rec.key))
        log.Printf("restore key[%s], value[%s], di: %+v", iter.Key(), iter.Value(), iter_di)

        if (err == nil && iter.df.id >= di.fileId) || err == ErrNotFound {
            // add to keydir
            err := bc.keyDir.Put(string(rec.key), iter_di)
            if err != nil {
                return nil, err
            }
        }

        // add to active keydir
        err = activeKD.Put(string(rec.key), iter_di)
        if err != nil {
            return nil, err
        }
    }
    return activeKD, nil
}

func (bc *BitCask) Get(key string) ([]byte, error) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    di, err := bc.keyDir.Get(key)
    if err != nil {
        return nil, err
    }

    if di.valueSize < 0 {
        log.Printf("key[%s] has been deleted", string(key))
        return nil, ErrNotFound
    }

    rec, err := bc.refRecord(di.fileId, di.valuePos - RecordValueOffset())
    if err != nil {
        log.Printf("ref file[%d] at offset[%d] failed, err=%s\n", di.fileId, di.valuePos - RecordValueOffset(), err)
        return nil, err
    }
    defer bc.unrefRecord(di.fileId, di.valuePos - RecordValueOffset())

    return rec.value, nil
}

func (bc *BitCask) Del(key string) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.version++

    rec := &Record{
        crc32: 0,
        keySize: int64(len(key)),
        valueSize: -1,
        key: make([]byte, len(key)),
        value: nil,
    }
    copy(rec.key, []byte(key))

    return bc.addRecord(rec)
}

func (bc *BitCask) Set(key string, val []byte) error {
    return bc.SetWithExpr([]byte(key), val, 0)
}

func (bc *BitCask) SetWithExpr(key []byte, value []byte, expration uint32) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.version++

    keySize := len(key)
    valueSize := len(value)
    rec := &Record{
        flag : 0,
        crc32: 0,
        expration: expration,
        keySize: int64(keySize),
        valueSize: int64(valueSize),
        key: make([]byte, keySize),
        value: make([]byte, valueSize),
    }
    copy(rec.key, []byte(key))
    copy(rec.value, value)

    return bc.addRecord(rec)
}

// requires bc.mu held
func (bc *BitCask) addRecord(rec *Record) error {
    offset := bc.activeFile.Size()
    err := bc.activeFile.AddRecord(rec)
    if err != nil {
        log.Println(err)
        return err
    }

    // update keyDir
    di := &DirItem{
        fileId: bc.activeFile.id,
        valuePos: offset + RecordValueOffset(),
        valueSize: rec.valueSize,
        expration: rec.expration,
    }
    err = bc.keyDir.Put(string(rec.key), di)
    if err != nil {
        log.Println(err)
        return err
    }

    err = bc.activeKD.Put(string(rec.key), di)
    if err != nil {
        log.Println(err)
        return err
    }

    if bc.activeFile.Size() >= bc.opts.maxFileSize {
        bc.rotateActiveFile()
    }
    return nil
}

func (bc *BitCask) rotateActiveFile() error {
    nextFileId := bc.activeFile.id + 1
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
        log.Println(err)
        return err
    }
    bc.activeFile = af

    return nil
}

func (bc *BitCask) generateHintFile(fileId int64) error {
    hf, err := NewHintFile(bc.getHintFilePath(fileId), fileId, bc.opts.bufferSize)
    if err != nil {
        return err
    }
    defer hf.Close()

    for key, di := range bc.activeKD.mp {
        hi := &HintItem{
            expration: di.expration,
            keySize: int64(len(key)),
            valueSize: di.valueSize,
            valuePos: di.valuePos,
            key: []byte(key),
        }
        err := hf.AddItem(hi)
        if err != nil {
            log.Println(err)
            return err
        }
    }
    return nil
}

func (bc *BitCask) Close() error {
    bc.activeFile.Close()
    if bc.recCache != nil {
        bc.recCache.Close()
    }
    if bc.dfCache != nil {
        bc.dfCache.Close()
    }
    return nil
}

func (bc *BitCask) Merge() {
    bc.merge()
}

func (bc *BitCask) merge() {
    if !atomic.CompareAndSwapInt32(&bc.isMerging, 0, 1) {
        log.Println("there is a merge process running.")
        return
    }
    defer atomic.CompareAndSwapInt32(&bc.isMerging, 1, 0)
    log.Println("start merge...")

    bc.mu.Lock()
    end := bc.activeFile.id
    bc.mu.Unlock()

    for begin := bc.minDataFileId; begin < end; begin = bc.NextDataFileId(begin) {
        err := bc.mergeDataFile(begin)
        if err != nil {
            log.Println("merge datafile[%d] failed, err=%s", begin, err)
            return
        }
    }
}

func (bc *BitCask) mergeDataFile(fileId int64) error {
    df, err := bc.dfCache.Ref(fileId)
    if err != nil {
        return err
    }
    defer bc.dfCache.Unref(fileId)

    begin := time.Now()
    iter := NewRecordIter(df)
    for iter.Reset(); iter.Valid(); iter.Next() {
        rec := iter.rec

        kdItem, _ := bc.keyDir.Get(string(rec.key))
        if kdItem != nil && kdItem.fileId == iter.df.id &&
            kdItem.valuePos - RecordValueOffset() == iter.recPos {
            // skip exprired key
            if int64(kdItem.expration) <= begin.Unix() {
                continue
            }

            err := bc.addRecord(iter.rec)
            if err != nil {
                log.Printf("merge record[%+v] failed, err=%s\n", iter.rec, err)
                return err
            }
        }
    }
    end := time.Now()

    // remove data file
    os.Remove(df.Path())

    log.Printf("merge data-file[%d] succ. costs %.2f seconds", fileId,
            end.Sub(begin).Seconds())
    return nil
}

func DestroyDatabase(dir string) error {
    log.Println("clearing db[%s]...", dir)

    d, err := os.Open(dir)
    if err != nil {
        return err
    }
    defer d.Close()
    names, err := d.Readdirnames(-1)
    if err != nil {
        return err
    }
    for _, name := range names {
        err = os.RemoveAll(filepath.Join(dir, name))
        if err != nil {
            return err
        }
    }

    log.Println("clear db[%s] succ!", dir)
    return nil
}

func (bc *BitCask) SyncFile(fileId int64, offset int64, length int64, reader io.Reader) error {
    path := bc.GetDataFilePath(fileId)
    f, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE, 0644)
    if err != nil {
        return err
    }

    _, err = f.Seek(offset, os.SEEK_SET)
    if err != nil {
        f.Close()
        return err
    }

    _, err = io.CopyN(f, reader, length)
    if err != nil {
        f.Close()
        return err
    }

    f.Close()

    // update keydir
    _, err = bc.restoreFromDataFile(path, fileId)
    if err != nil {
        return err
    }

    if fileId > bc.maxDataFileId {
        bc.maxDataFileId = fileId
    }

    return nil
}

func (bc *BitCask) EnableCache(enable bool) {
    if enable {
        bc.recCache = NewRecordCache(bc)
        bc.dfCache = NewDataFileCache(bc)
        // TODO recreate active file

        fileId := bc.maxDataFileId
        path := bc.GetDataFilePath(fileId)
        var err error
        bc.activeFile, err = NewActiveFile(path, fileId, bc.opts.bufferSize)
        if err != nil {
            log.Fatal(err)
            return
        }
        bc.activeKD, err = bc.restoreFromDataFile(path, fileId)
        if err != nil {
            log.Fatal(err)
            return
        }
    } else {
        if bc.activeFile != nil {
            bc.activeFile.Close()
            bc.activeFile = nil
            bc.activeKD = nil
        }
        if bc.recCache != nil {
            bc.recCache.Close()
            bc.recCache = nil
        }
        if bc.dfCache != nil {
            bc.dfCache.Close()
            bc.dfCache = nil
        }
    }
}

