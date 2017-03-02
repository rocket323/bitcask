package bitcask

type SnapshotIter struct {
    snap        *Snapshot
    curFileId   int64
    curPos      int64
    valid       bool
}

func (sp *Snapshot) NewSnapIter() *SnapshotIter {
}

func (it *SnapshotIter) SeekToFirst() error {
}

