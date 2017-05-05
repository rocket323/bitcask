package bitcask

import (
    "time"
    "math/rand"
    "fmt"
    "testing"
    . "gopkg.in/check.v1"
)

type testBitCaskSuite struct {
    bc *BitCask
}

var _ = Suite(&testBitCaskSuite{})
var value = make([]byte, 10240)

func Test(t *testing.T) { TestingT(t) }

func init() {
    rand.Seed(time.Now().UnixNano())
}

func (s *testBitCaskSuite) SetUpSuite(c *C) {
    path := c.MkDir()
    opts := NewOptions()
    opts.maxFileSize = 10 * 1024 * 1024
    var err error
    s.bc, err = Open(path, opts)
    c.Assert(err, IsNil)
}

func (s *testBitCaskSuite) TearDownSuite(c *C) {
    s.bc.Close()
}

func testRandomKey() string {
    return fmt.Sprintf("%09d", rand.Int() % 1000)
}

func (s *testBitCaskSuite) TestMerge(c *C) {
    n := 10240
    keys := make(map[string]bool)

    for i := 0; i < n; i++ {
        key := testRandomKey()
        err := s.bc.Set(key, value)
        c.Assert(err, IsNil)
        keys[key] = true
    }

    done := make(chan int, 1)
    s.bc.Merge(done)

    for i := 0; i < n; i++ {
        key := testRandomKey()
        err := s.bc.Set(key, value)
        c.Assert(err, IsNil)
        keys[key] = true
    }

    <-done

    fmt.Printf("done\n")
    for k, _ := range keys {
        _, err := s.bc.Get(k)
        c.Assert(err, IsNil)
    }
}

