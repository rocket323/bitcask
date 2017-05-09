package bitcask

import (
    "time"
    "log"
    "sync/atomic"
)

func (bc *BitCask) Merge(done chan int, files chan int64) {
    go bc.merge(done, files)
}

func (bc *BitCask) merge(done chan int, files chan int64) {
    if !atomic.CompareAndSwapInt32(&bc.isMerging, 0, 1) {
        log.Println("there is a merge process running.")
        return
    }
    defer atomic.CompareAndSwapInt32(&bc.isMerging, 1, 0)
    log.Println("start merge...")

    begin := time.Now()
    bc.mu.Lock()
    end := bc.activeFile.id
    bc.mu.Unlock()

    for begin := bc.minDataFileId; begin < end; begin = bc.NextDataFileId(begin) {
        err := bc.mergeDataFile(begin)
        if err != nil {
            log.Println("merge datafile[%d] failed, err=%s", begin, err)
            return
        }
        if files != nil {
            files <- begin
        }
    }
    d := time.Now().Sub(begin)
    log.Printf("merge succ. cost %.2f seconds", d.Seconds())
    done <- 1
    if files != nil {
        close(files)
    }
}

func (bc *BitCask) mergeDataFile(fileId int64) error {
    df, err := bc.dfCache.Ref(fileId)
    if err != nil {
        return err
    }
    defer bc.dfCache.Unref(fileId)

    begin := time.Now()
    err = df.ForEachItem(func (rec *Record, offset int64) error {
        kdItem, _ := bc.keyDir.Get(rec.key)
        if kdItem != nil && kdItem.fileId == df.id &&
                int64(kdItem.valuePos) - RecordValueOffset() == offset {
            // skip exprired key
            if int64(kdItem.expration) <= begin.Unix() {
                return nil
            }

            err := bc.AddRecord(rec, false)
            if err != nil {
                return err
            }
        }
        return nil
    })

    if err != nil {
        return err
    }
    end := time.Now()

    // remove data file and hint file
    if err := bc.removeDataFile(fileId); err != nil {
        log.Fatalf("remove data-file[%d] failed, err = %s", fileId, err)
        return err
    }

    log.Printf("merge data-file[%d] succ. costs %.2f seconds", fileId,
            end.Sub(begin).Seconds())
    return nil
}

