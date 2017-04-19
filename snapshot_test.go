package bitcask_test

import (
    _ "fmt"
    _ "testing"
    _ "github.com/rocket323/bitcask"
)

/*
func TestSnapshotSimple(t *testing.T) {
    opts := bitcask.NewOptions()
    bc, err := bitcask.Open("test", opts)
    if err != nil {
        t.Error("open db failed, err=", err)
    }

    for i := 0; i < 10; i++ {
        key := fmt.Sprintf("key_%d", i)
        value := []byte(fmt.Sprintf("value_%d", i))
        err := bc.Set(key, value)
        if err != nil {
            t.Errorf("set %s, failed\n", key)
        }
    }

    snap := bc.NewSnapshot()
    defer bc.ReleaseSnapshot(snap)

    err = bc.Set("key_5", []byte("new_value"))
    if err != nil {
        t.Errorf("replace key_5 failed, err=%s", err)
    }

    it := snap.NewSnapshotIter()
    for it.SeekToFirst(); it.Valid(); it.Next() {
        key := string(it.Key())
        value := string(it.Value())
        if key == "key_5" {
            if value != "value_5" {
                t.Error("snapshot value invalid")
            }
        }
    }

    err = bitcask.DestroyDatabase("test")
    if err != nil {
        t.Errorf("destroy database test failed, err=%+v\n", err)
    }
}
*/

