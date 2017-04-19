package bitcask_test

import (
    "log"
    "fmt"
    "os"
    "testing"
    "github.com/rocket323/bitcask"
)

func init() {
    log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func TestBitCaskSimple(t *testing.T) {
    dbpath := "unittest_db"

    if err := os.Mkdir(dbpath, os.ModePerm); err != nil {
        t.Error("mkdir failed, err=", err)
    }
    defer func() {
        fmt.Printf("remove %s\n", dbpath)
        err := os.RemoveAll(dbpath)
        if err != nil {
            t.Error("remove dir failed, err=", err)
        }
    }()

    opts := bitcask.NewOptions()
    bc, err := bitcask.Open(dbpath, opts)
    if err != nil {
        t.Error("open bitcask failed")
    }

    err = bc.Set("key1", []byte("hello"))
    if err != nil {
        t.Error(err)
    }

    err = bc.Set("key2", []byte("world"))
    if err != nil {
        t.Error(err)
    }

    err = bc.Set("key3", []byte("nihao"))
    if err != nil {
        t.Error(err)
    }

    val, err := bc.Get("key3")
    if err != nil {
        t.Error(err)
    }
    if string(val) != "nihao" {
        t.Errorf("content[%s] invalid\n", string(val))
    }

    val, err = bc.Get("key2")
    if err != nil {
        t.Error(err)
    }
    if string(val) != "world" {
        t.Errorf("content[%s] invalid\n", string(val))
    }

    _, err = bc.Get("key4")
    if err != bitcask.ErrNotFound {
        t.Error("content invalid")
    }

    err = bc.Del("key2")
    if err != nil {
        t.Error(err)
    }

    _, err = bc.Get("key2")
    if err != bitcask.ErrNotFound {
        t.Error("content invalid")
    }
}

