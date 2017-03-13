package bitcask

import (
    "strings"
    "path/filepath"
    "fmt"
    "strconv"
    "io/ioutil"
    "log"
    "os"
)

func GetBaseFromId(id int64) string {
    if id < 0 {
        return "INVALID"
    }
    return fmt.Sprintf("%09d", id)
}

func GetIdFromPath(path string) (int64, error) {
    base := filepath.Base(path)
    name := strings.TrimSuffix(base, filepath.Ext(base))
    id, err := strconv.ParseInt(name, 10, 64)
    return id, err
}

func (bc *BitCask) GetDataFilePath(id int64) string {
    return bc.dir + "/" + GetBaseFromId(id) + ".data"
}

func (bc *BitCask) NewDataFileFromId(id int64) (*DataFile, error) {
    path := bc.GetDataFilePath(id)
    df, err := NewDataFile(path, id)
    if err != nil {
        return nil, err
    }
    return df, nil
}

func (bc *BitCask) FirstFileId() int64 {
    files, err := ioutil.ReadDir(bc.dir)
    if err != nil {
        log.Println(err)
        return -1
    }

    minId := int64(-1)
    for _, file := range files {
        id, err := GetIdFromPath(file.Name())
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
        name := bc.GetDataFilePath(id)
        if _, err := os.Stat(name); os.IsNotExist(err) {
            continue
        }
        break
    }
    return id
}
