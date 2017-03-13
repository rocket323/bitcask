package lru_test

import (
    _ "fmt"
    "testing"
    "github.com/rocket323/bitcask/lru"
)

type recKey struct {
    a   int
    b   int
}

type Record struct {
    str string
    num int
}

func TestSimple(t *testing.T) {
    c := lru.NewCache(100, nil)
    defer c.Close()
    c.Put(1, "nihao")
    c.Put(2, "hello")


    _, err := c.Ref(1)
    if err != nil {
        t.Errorf("get failed")
    }

    cc := lru.NewCache(100, nil)
    defer cc.Close()
    cc.Put(recKey{0, 1}, Record{"hello", 10})
    cc.Put(recKey{2, 3}, Record{"world", 100})
    cc.Put(recKey{3, 3}, Record{"world", 100})
    cc.Put(recKey{-1, 3}, Record{"world", 100})

    _, err = cc.Ref(recKey{3, 3})
    if err != nil {
        t.Errorf("get failed")
    }
}

func TestEvit(t *testing.T) {
    c := lru.NewCache(5, nil)
    defer c.Close()
    c.Put(1, "nihao")
    c.Put(2, "nihao")
    c.Put(3, "nihao")
    c.Put(4, "nihao")
    c.Put(5, "nihao")

    c.Put(6, "nihao")

    _, err := c.Ref(1)
    if err != lru.ErrNotInCache {
        t.Errorf("get failed, err=%+v\n", err)
    }
}

func TestNotEvitWhenRef(t *testing.T) {
    c := lru.NewCache(5, nil)
    defer c.Close()
    c.Put(1, "nihao")
    c.Ref(1)
    c.Put(2, "nihao")
    c.Put(3, "nihao")
    c.Put(4, "nihao")
    c.Put(5, "nihao")

    c.Put(6, "nihao")

    _, err := c.Ref(1)
    if err != nil {
        t.Errorf("get failed, err=%+v\n", err)
    }
}

func TestNotEvitAfterUnref(t *testing.T) {
    c := lru.NewCache(5, nil)
    defer c.Close()
    c.Put(1, "nihao")
    c.Ref(1)
    c.Unref(1)
    c.Put(2, "nihao")
    c.Put(3, "nihao")
    c.Put(4, "nihao")
    c.Put(5, "nihao")

    c.Put(6, "nihao")

    _, err := c.Ref(1)
    if err != lru.ErrNotInCache {
        t.Errorf("get failed, err=%+v\n", err)
    }
}
