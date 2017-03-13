package bitcask

import (
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
    opts        *Options
    version     uint64
    snaps       map[uint64]*Snapshot
    recCache    *RecordCache
    dfCache     *DataFileCache
    isMerging   int32
}

func Open(dir string, opts *Options) (*BitCask, error) {
    bc := &BitCask{
        dir: dir,
        keyDir: NewKeyDir(),
        activeFile: nil,
        mu: &sync.RWMutex{},
        opts: opts,
        version: 0,
        snaps: make(map[uint64]*Snapshot),
        isMerging: 0,
    }
    bc.recCache = NewRecordCache(int(opts.cacheSize), bc)
    bc.dfCache = NewDataFileCache(int(opts.maxOpenFiles), bc)

    err := bc.Restore()
    if err != nil {
        log.Println(err)
        return nil, err
    }
    return bc, nil
}

func (bc *BitCask) Restore() error {
    files, err := ioutil.ReadDir(bc.dir)
    if err != nil {
        log.Println(err)
        return err
    }

    lastId := int64(0)
    for _, file := range files {
        id, err := GetIdFromPath(file.Name())
        if err != nil {
            log.Printf("invalid dataFile name[%s], skip\n", file.Name())
            continue
        }

        df, err := NewDataFile(bc.GetDataFilePath(id), id)
        if err != nil {
            log.Println(err)
            return err
        }

        iter := NewRecordIter(df, bc)
        if err != nil {
            log.Println(err)
            return err
        }
        for iter.Reset(); iter.Valid(); iter.Next() {
            rec := iter.curRec
            curDirItem := &DirItem{
                fileId: iter.df.id,
                recOffset: iter.curPos,
                timeStamp: rec.timeStamp,
            }
            di, err := bc.keyDir.Get(string(rec.key))
            if (err == nil && iter.df.id >= di.fileId) || err == ErrNotFound {
                err := bc.keyDir.Put(string(rec.key), curDirItem)
                if err != nil {
                    log.Println(err)
                    return err
                }
            }
            // log.Printf("restore key[%s]", string(rec.key))
        }

        if df.id > lastId {
            lastId = df.id
        }
    }
    log.Printf("restore db[%s] succ, lastId[%d]\n", bc.dir, lastId)

    // make active file
    bc.activeFile, err = NewActiveFile(bc.GetDataFilePath(lastId), lastId, bc.opts.bufferSize)
    if err != nil {
        log.Println(err)
        return err
    }

    return nil
}

func (bc *BitCask) Get(key string) ([]byte, error) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    di, err := bc.keyDir.Get(key)
    if err != nil {
        return nil, err
    }

    rec, err := bc.recCache.Ref(di.fileId, di.recOffset)
    if err != nil {
        return nil, err
    }
    return rec.value, nil
}

func (bc *BitCask) Del(key string) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.version++

    rec := &Record{
        crc32: 0,
        timeStamp: uint32(time.Now().Unix()),
        keySize: int64(len(key)),
        valueSize: -1,
        key: make([]byte, len(key)),
        value: nil,
    }
    copy(rec.key, []byte(key))

    return addRecord(rec)
}

func (bc *BitCask) Set(key string, val []byte) error {
    return bc.SetWithExpration([]byte(key), val, 0)
}

func (bc *BitCask) SetWithExpr(key []byte, value []byte, expration uint32) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.version++

    keySize := len(key)
    valueSize := len(val)
    rec := &Record{
        crc32: 0,
        expration: expration,
        keySize: int64(keySize),
        valueSize: int64(valueSize),
        key: make([]byte, keySize),
        value: make([]byte, valueSize),
    }
    copy(rec.key, []byte(key))
    copy(rec.value, val)

    return addRecord(rec)
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
        fileId: bc.activeFile.di,
        recOffset: offset,
        expration: rec.expration,
    }
    err = bc.keyDir.Put(key, di)
    if err != nil {
        log.Println(err)
        return err
    }

    if bc.activeFile.Size() >= bc.opts.maxFileSize {
        nextFileId := bc.activeFile.id + 1
        bc.activeFile.Close()
        af, err := NewActiveFile(bc.GetDataFilePath(nextFileId), nextFileId, bc.opts.bufferSize)
        if err != nil {
            log.Println(err)
            return err
        }
        bc.activeFile = af
    }
    return nil
}

func (bc *BitCask) Close() error {
    bc.activeFile.Close()
    bc.recCache.Close()
    bc.dfCache.Close()
    return nil
}

func (bc *BitCask) Merge() {
    bc.merge()
}

func (bc *BitCask) merge() {
    if !CompareAndSwapInt32(&bc.isMerging, 0, 1) {
        log.Println("there is a merge process running.")
        return nil
    }
    defer CompareAndSwapInt32(&bc.isMerging, 1, 0)
    log.Println("start merge...")

    bc.mu.Lock()
    end := bc.activeFile.id
    bc.mu.Unlock()

    for begin := bc.minDataFileId; begin < end; begin++ {
        err := mergeDataFile(begin)
        if err != nil {
            log.Println("merge data-file[%d] failed, err=%s", begin, err)
            return
        }
    }
}

func (bc *BitCask) mergeDataFile(fileId int64) error {
    df, err := bc.dfCache.Ref(fileId)
    if err != nil {
        return err
    }

    begin := time.Now()
    iter := NewRecordIter(df, bc)
    for iter.Reset(); iter.Valid(); iter.Next() {

        err := bc.addRecord(iter.curRec)
        if err != nil {
            log.Printf("merge record[%+v] failed, err=%s\n", iter.curRec, err)
            return err
        }
    }
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

