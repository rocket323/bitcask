package bitcask

import (
    "strings"
    "path/filepath"
    "fmt"
    "strconv"
)

func GetIdFromName(name string) (int64, error) {
    base := strings.TrimSuffix(name, filepath.Ext(name))
    id, err := strconv.ParseInt(base, 10, 64)
    return id, err
}

func GetBaseFromId(id int64) string {
    if id < 0 {
        return "INVALID"
    }
    return fmt.Sprintf("%09d", id)
}

