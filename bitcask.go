package bitcask

import (
    "fmt"
    "sync"
    "io/ioutil"
    "log"
    "time"
    "os"
    "path/filepath"
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
}

func Open(dir string, opts *Options) (*BitCask, error) {
    bc := &BitCask {
        dir: dir,
        keyDir: NewKeyDir(),
        activeFile: nil,
        mu: &sync.RWMutex{},
        opts: opts,
        version: 0,
        snaps: make(map[uint64]*Snapshot),
    }
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

    lastId := int64(-1)
    for _, file := range files {
        id, err := GetIdFromName(file.Name())
        if err != nil {
            log.Printf("invalid dataFile name[%s], skip\n", file.Name())
            continue
        }

        raf, err := NewRandomAccessFile(bc.GetDataFileName(id), id, false)
        if err != nil {
            log.Println(err)
            return err
        }

        iter := NewRecordIter(raf)
        if err != nil {
            log.Println(err)
            return err
        }
        for iter.Reset(); iter.Valid(); iter.Next() {
            rec := iter.curRec
            curDirItem := &DirItem {
                fileId: iter.f.id,
                valueSize: rec.valueSize,
                valuePos: iter.curPos + rec.ValueFieldOffset(),
                timeStamp: rec.timeStamp,
            }
            di, err := bc.keyDir.Get(string(rec.key))
            if (err == nil && iter.f.id >= di.fileId) || err == ErrNotFound {
                err := bc.keyDir.Put(string(rec.key), curDirItem)
                if err != nil {
                    log.Println(err)
                    return err
                }
            }
            // log.Printf("restore key[%s]", string(rec.key))
        }

        if raf.id > lastId {
            bc.activeFile = &ActiveFile{raf}
            lastId = raf.id
        }
    }
    log.Printf("restore db[%s] succ, lastId[%d]\n", bc.dir, lastId)

    if lastId == -1 {
        raf, err := NewRandomAccessFile(bc.GetDataFileName(0), 0, true)
        if err != nil {
            log.Println(err)
            return err
        }
        bc.activeFile = &ActiveFile{raf}
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

    raf, err := bc.NewDataFileFromId(di.fileId, false)
    if err != nil {
        log.Println(err)
        return nil, err
    }
    defer raf.Close()

    data, err := raf.ReadAt(di.valuePos, di.valueSize)
    if err != nil {
        log.Println(err, di.valuePos, di.valueSize)
        return nil, err
    }

    return data, nil
}

func (bc *BitCask) Set(key string, val []byte) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.version++

    keySize := len(key)
    valueSize := len(val)
    rec := &Record {
        crc32: 0,
        timeStamp: uint32(time.Now().Unix()),
        keySize: int64(keySize),
        valueSize: int64(valueSize),
        key: make([]byte, keySize),
        value: make([]byte, valueSize),
    }
    copy(rec.key, []byte(key))
    copy(rec.value, val)
    offset := bc.activeFile.Size()
    err := bc.activeFile.AddRecord(rec)
    if err != nil {
        log.Println(err)
        return err
    }

    // update keyDir
    di := &DirItem {
        fileId: bc.activeFile.id,
        valueSize: int64(len(val)),
        valuePos: offset + RECORD_HEADER_SIZE + int64(keySize),
        timeStamp: rec.timeStamp,
    }
    err = bc.keyDir.Put(key, di)
    if err != nil {
        log.Println(err)
        return err
    }

    // new active file
    if bc.activeFile.Size() >= bc.opts.maxFileSize {
        nextFileId := bc.activeFile.id + 1
        bc.activeFile.Close()
        raf, err := NewRandomAccessFile(bc.GetDataFileName(nextFileId), nextFileId, true)
        if err != nil {
            log.Println(err)
            return err
        }
        bc.activeFile = &ActiveFile{raf}
    }

    return nil
}

func (bc *BitCask) Del(key string) error {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    bc.version++

    rec := &Record {
        crc32: 0,
        timeStamp: uint32(time.Now().Unix()),
        keySize: int64(len(key)),
        valueSize: -1,
        key: make([]byte, len(key)),
        value: nil,
    }
    copy(rec.key, []byte(key))
    err := bc.activeFile.AddRecord(rec)
    if err != nil {
        log.Println(err)
        return err
    }

    err = bc.keyDir.Del(key)
    if err != nil {
        log.Println(err)
        return err
    }

    if bc.activeFile.Size() >= bc.opts.maxFileSize {
        nextFileId := bc.activeFile.id + 1
        bc.activeFile.Close()
        raf, err := NewRandomAccessFile(bc.GetDataFileName(nextFileId), nextFileId, true)
        if err != nil {
            log.Println(err)
            return err
        }
        bc.activeFile = &ActiveFile{raf}
    }

    return nil
}

func (bc *BitCask) ListKeys() ([]string, error) {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    var keySet = make([]string, 0)
    for k, _ := range bc.keyDir.mp {
        keySet = append(keySet, k)
    }
    return keySet, nil
}

func (bc *BitCask) Close() error {
    bc.activeFile.Close()
    return nil
}

func (bc *BitCask) Merge() error {
    return nil
}

func (bc *BitCask) GetDataFileName(id int64) string {
    return bc.dir + "/" + GetBaseFromId(id) + ".data"
}

func (bc *BitCask) NewDataFileFromId(id int64, create bool) (*RandomAccessFile, error) {
    name := bc.GetDataFileName(id)
    return NewRandomAccessFile(name, id, create)
}

func (bc *BitCask) FirstFileId() int64 {
    files, err := ioutil.ReadDir(bc.dir)
    if err != nil {
        log.Println(err)
        return -1
    }

    minId := int64(-1)
    for _, file := range files {
        id, err := GetIdFromName(file.Name())
        if err != nil {
            log.Printf("invalid dataFile name[%s], skip\n", file.Name())
            continue
        }
        if minId == -1 || id < minId {
            minId = id
        }
    }
    return minId
}

func (bc *BitCask) NextFileId(id int64) int64 {
    for {
        id++
        if id > bc.activeFile.id {
            id = -1
            break
        }
        name := bc.GetDataFileName(id)
        if _, err := os.Stat(name); os.IsNotExist(err) {
            continue
        }
        break
    }
    return id
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

