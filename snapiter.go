package bitcask

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
    firstId := sp.bc.FirstFileId()
    if firstId == -1 {
        it.valid = false
        return ErrNotFound
    }
    raf, err := sp.bc.NewDataFileFromId(firstId, false)
    if err != nil {
        it.valid = false
        return ErrNotFound
    }

    it.recIter = NewRecordIter(raf)
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
            it.valid = false
            return
        }
        raf, err := it.snap.bc.NewDataFileFromId(nextFileId, false)
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

