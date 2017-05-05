package bitcask

import (
    "os"
    "time"
    "log"
    "sync/atomic"
)

func (bc *BitCask) Merge(done chan int) {
    go bc.merge(done)
}

func (bc *BitCask) merge(done chan int) {
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
    }
    d := time.Now().Sub(begin)
    log.Printf("merge succ. cost %.2f seconds", d.Seconds())

    done <- 1
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

            err := bc.AddRecord(iter.rec, false)
            if err != nil {
                log.Printf("merge record[%+v] failed, err=%s\n", iter.rec, err)
                return err
            }
        }
    }
    end := time.Now()

    // remove data file and hint file
    os.Remove(df.Path())
    os.Remove(bc.getHintFilePath(fileId))

    log.Printf("merge data-file[%d] succ. costs %.2f seconds", fileId,
            end.Sub(begin).Seconds())
    return nil
}
