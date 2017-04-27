package bitcask

import (
    "os"
    "strings"
    "path/filepath"
    "fmt"
    "strconv"
    "log"
)

type Env interface {
    getOptions() *Options
    getRecordCache() *RecordCache
    getDataFileCache() *DataFileCache

    getActiveFile() *ActiveFile
    GetDataFilePath(fileId int64) string
    getHintFilePath(fileId int64) string
    GetMinDataFileId() int64
    NextDataFileId(fileId int64) int64

    refDataFile(fileId int64) (*DataFile, error)
    unrefDataFile(fileId int64)
    refRecord(fileId int64, offset int64) (*Record, error)
    unrefRecord(fileId int64, offset int64)
}

func getBaseFromId(id int64) string {
    if id < 0 {
        log.Fatalf("id[%d] < 0, invalid fileId", id)
    }
    return fmt.Sprintf("%09d", id)
}

func getIdFromDataPath(path string) (int64, error) {
    base := filepath.Base(path)
    name := strings.TrimSuffix(base, filepath.Ext(base))
    if filepath.Ext(base) != ".data" {
        return 0, ErrInvalid
    }
    id, err := strconv.ParseInt(name, 10, 64)
    return id, err
}

func (bc *BitCask) getOptions() *Options {
    return bc.opts
}
func (bc *BitCask) getRecordCache() *RecordCache {
    return bc.recCache
}
func (bc *BitCask) getDataFileCache() *DataFileCache {
    return bc.dfCache
}

func (bc *BitCask) GetDataFilePath(id int64) string {
    return bc.dir + "/" + getBaseFromId(id) + ".data"
}
func (bc *BitCask) getHintFilePath(id int64) string {
    return bc.dir + "/" + getBaseFromId(id) + ".hint"
}
func (bc *BitCask) GetMinDataFileId() int64 {
    return bc.minDataFileId
}

func (bc *BitCask) NextDataFileId(fileId int64) int64 {
    for {
        fileId++
        path := bc.GetDataFilePath(fileId)
        if _, err := os.Stat(path); err == nil {
            break
        }
    }
    return fileId
}

func (bc *BitCask) ActiveFileId() int64 {
    return bc.activeFile.id
}

func (bc *BitCask) getActiveFile() *ActiveFile {
    return bc.activeFile
}

func (bc *BitCask) refDataFile(fileId int64) (*DataFile, error) {
    return bc.dfCache.Ref(fileId)
}
func (bc *BitCask) unrefDataFile(fileId int64) {
    bc.dfCache.Unref(fileId)
}

func (bc *BitCask) refRecord(fileId int64, offset int64) (*Record, error) {
    return bc.recCache.Ref(fileId, offset)
}
func (bc *BitCask) unrefRecord(fileId int64, offset int64) {
    bc.recCache.Unref(fileId, offset)
}

