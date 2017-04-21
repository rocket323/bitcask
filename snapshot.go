package bitcask

import (
    "log"
    "container/list"
)

type Snapshot struct {
    bc                      *BitCask
    snapId                  uint64
    activeFileId            int64
    lastActiveSize          int64
    iters                   *list.List
}

func (bc *BitCask) NewSnapshot() *Snapshot {
    bc.mu.Lock()
    defer bc.mu.Unlock()

    snap := &Snapshot {
        bc: bc,
        snapId: bc.version,
        activeFileId: bc.activeFile.id,
        lastActiveSize: bc.activeFile.Size(),
        iters: list.New(),
    }
    bc.snaps[snap.snapId] = snap
    bc.version++
    return snap
}

func (bc *BitCask) ReleaseSnapshot(snap *Snapshot) {
    if snap == nil {
        log.Fatal("snap is nil")
        return
    }
    bc.mu.Lock()
    defer bc.mu.Unlock()

    if snap.iters.Len() > 0 {
        log.Fatal("snapshot[%d] is referenced by iterators yet!", snap.snapId)
        return
    }
    delete(bc.snaps, snap.snapId)
}

/////////////////////////////////////////////

type SnapshotIter struct {
    snap        *Snapshot
    recIter     *RecordIter
    valid       bool
}

func (sp *Snapshot) NewSnapshotIter() *SnapshotIter {
    spIter := &SnapshotIter {
        snap: sp,
        recIter: nil,
        valid: false,
    }
    return spIter
}

func (it *SnapshotIter) SeekToFirst() error {
    sp := it.snap
    it.valid = true
    firstId := sp.bc.getMinDataFileId()
    if firstId == -1 {
        it.valid = false
        return ErrNotFound
    }
    path := sp.bc.getDataFilePath(firstId)
    df, err := NewDataFile(path, firstId)
    if err != nil {
        it.valid = false
        return ErrNotFound
    }

    it.recIter = NewRecordIter(df)
    it.recIter.Reset()
    return nil
}

func (it *SnapshotIter) Valid() bool {
    // valid check
    validCheck := it.valid
    if !it.recIter.Valid() {
        validCheck = false
    }
    if !validCheck {
        return false
    }

    // active offset check
    if it.recIter.df.id > it.snap.activeFileId {
        return false
    }
    if it.recIter.df.id == it.snap.activeFileId {
        if it.recIter.recPos >= it.snap.lastActiveSize {
            return false
        }
    }
    return true
}

func (it *SnapshotIter) Next() {
    if !it.valid {
        return
    }
    it.recIter.Next()
    bc := it.snap.bc

    if !it.recIter.Valid() { // move to next file
        it.recIter.Close()
        nextFileId := bc.nextDataFileId(it.recIter.df.id)
        if nextFileId == -1 {
            it.valid = false
            return
        }
        path := bc.getDataFilePath(nextFileId)
        raf, err := NewDataFile(path, nextFileId)
        if err != nil {
            it.valid = false
            return
        }
        it.recIter = NewRecordIter(raf)
        it.recIter.Reset()
    }
}

func (it *SnapshotIter) Close() {
    it.recIter.Close()
}

func (it *SnapshotIter) Key() []byte {
    return it.recIter.Key()
}

func (it *SnapshotIter) Value() []byte {
    return it.recIter.Value()
}

