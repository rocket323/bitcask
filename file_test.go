package bitcask

import (
    "testing"
)

func TestOpen(t *testing.T) {
    f, err := NewRandomAccessFile("hello.data")
    if err != nil {
        t.Error(err)
    }

    p := []byte("hello world\n")
    err = f.Append(p)
    if err != nil {
        t.Error(err)
    }

    a := []byte("ni hao")
    err = f.WriteAt(0, a)
    if err != nil {
        t.Error(err)
    }

    x, err := f.ReadAt(0, 6)
    if err != nil {
        t.Error(err)
    }
    t.Log(x)

    err = f.Close()
    if err != nil {
        t.Error(err)
    }
}

