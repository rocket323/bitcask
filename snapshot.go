package bitcask

import (
    "log"
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
    }
    bc.snaps.PushBack(snap)
    bc.version++
    return snap
}

func (bc *BitCask) releaseSnapshot(snap *Snapshot) {
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

