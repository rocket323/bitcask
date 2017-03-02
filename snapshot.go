package bitcask

import (
    "log"
)

type Snapshot struct {
    activeFileId            int64
    lastActiveOffset        int64
}

func (bc *BitCask) NewSnapshot() *Snapshot {
}

func (bc *BitCask) ReleaseSnapshot() {
}

