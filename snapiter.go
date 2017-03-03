package bitcask

type SnapshotIter struct {
    snap        *Snapshot
    recIter     *RecordIter
    valid       bool
}

func (sp *Snapshot) NewSnapIter() *SnapshotIter {
    spIter := &SnapshotIter {
        snap: sp,
        recIter: nil,
        valid: false,
    }
    return spIter
}

func (it *SnapshotIter) SeekToFirst() error {
    sp := it.snap
    firstId := sp.bc.FirstFileId()
    if firstId == -1 {
        valid = false
        return ErrNotFound
    }
    raf, err := sp.bc.NewDataFileFromId(firstId)
    if err != nil {
        valid = false
        return ErrNotFound
    }

    it.recIter = NewRecordIter(raf)
    it.recIter.Reset()
}

func (it *SnapshotIter) Valid() bool {
    validCheck := valid
    if !it.recIter.Valid() {
        validCheck = false
    }
    if !validCheck {
        return false
    }

    // active offset check
    if it.recIter.f.id > it.snap.activeFileId {
        return false
    }
    if it.recIter.f.id == it.snap.activeFileId {
        if it.recIter.curPos >= it.snap.lastActiveSize {
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

    if !it.recIter.Valid() { // move to next file
        it.recIter.Close()
        nextFileId := it.snap.bc.NextFileId(it.recIter.f.id)
        if nextFileId == -1 {
            valid = false
            return
        }
        raf, err := it.snap.bc.NewDataFileFromId(nextFileId)
        if err != nil {
            valid = false
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

