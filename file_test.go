package bitcask_test

import (
    "io"
    "fmt"
    "testing"
    "github.com/rocket323/bitcask"
    "os"
)

func TestFileSimple(t *testing.T) {
    f, err := bitcask.NewFileWithBuffer("data.out", true, 10)
    if err != nil {
        t.Error("open file failed, err=", err)
    }
    defer func() {
        fmt.Println("remove data.out")
        err := os.Remove("data.out")
        if err != nil {
            t.Error("remove file failed", err)
        }
    }()
    defer f.Close()

    sa := []byte("ni hao aaaaa")
    sb := []byte("hello world bbbbb")

    _, err = f.Write(sa)
    if err != nil {
        t.Error("write bytes failed")
    }

    _, err = f.Write(sb)
    if err != nil {
        t.Error("write bytes failed")
    }

    data, err := f.ReadAt(3, 5)
    if err != nil {
        t.Error("read failed")
    }
    if (string(data) != "hao a") {
        t.Error("content invalid")
    }

    data, err = f.ReadAt(12, 11)
    if err != nil {
        t.Error("read failed")
    }
    if (string(data) != "hello world") {
        t.Error("content invalid")
    }

    data, err = f.ReadAt(12, 100)
    if err != io.EOF {
        t.Error("read failed")
    }
    if (string(data) != "hello world bbbbb") {
        t.Error("content invalid")
    }

    if int(f.Size()) != len(sa) + len(sb) {
        t.Error("length invalid")
    }
}

func TestFileLargeBuffer(t *testing.T) {
    f, err := bitcask.NewFileWithBuffer("data.out", true, 1000)
    if err != nil {
        t.Error("open file failed, err=", err)
    }
    defer func() {
        fmt.Println("remove data.out")
        err := os.Remove("data.out")
        if err != nil {
            t.Error("remove file failed", err)
        }
    }()
    defer f.Close()

    _, err = f.Write([]byte("abcdefghi"))
    if err != nil {
        t.Error(err)
    }

    _, err = f.Write([]byte("xxyyzz"))
    if err != nil {
        t.Error(err)
    }

    data, err := f.ReadAt(0, 9)
    if err != nil {
        t.Error(err)
    }
    if string(data) != "abcdefghi" {
        t.Errorf("content[%s] invalid\n", string(data))
    }

    data, err = f.ReadAt(12, 3)
    if err != nil {
        t.Error(err)
    }
    if string(data) != "yzz" {
        t.Errorf("content[%s] invalid\n", string(data))
    }
}

