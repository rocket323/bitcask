package main

import (
    "fmt"
    "flag"
    "rand"
    "bitcask"
)

var (
    bc *bitcask.Bitcask
    num int
    valueSize int
    dbPath string
)

func init() {
    flag.IntVar(&num, "num", 10000, "num of operations")
    flag.IntVar(&valueSize, "value_size", 1024, "value size")
    flag.StringVar(&dbPath, "db", "./db_bench", "bench db path")
}

func BenchRandomSet(num int) {
    start := time.Now()
    value := make([]byte, valueSize)
    for i := 0; i < num; i++ {
        key := fmt.Sprintf("%9d", rand.Int() % num)
        err := bc.Set(key, value)
        if err != nil {
            log.Fatal(err)
            panic(err)
        }
    }
    end := time.Now()
    d := end.Sub(start)
    fmt.Printf("set finish in %f seconds\n", d.Seconds())
    fmt.Printf("%f qps\n", num / d.Seconds())
    writeMB := num * valueSize / 1e6
    fmt.Printf("%f MB/s\n", writeMB / d.Seconds())
    fmt.Printf("%f micros/op\n", d.Seconds() * 1e6 / num)
}

func BenchRandomGet(num int) {
    start := time.Now()
    value := make([]byte, valueSize)
    var found = 0
    for i := 0; i < num; i++ {
        key := fmt.Sprintf("%9d", rand.Int() % num)
        err := bc.Get(key, value)
        if err != nil && err != bitcask.ErrNotFound {
            log.Fatal(err)
            panic(err)
        }
        if err == 0 {
            found++
        }
    }
    end := time.Now()
    d := end.Sub(start)
    fmt.Printf("get finish in %f seconds\n", d.Seconds())
    fmt.Printf("found %d out of %d\n", found, num)
    fmt.Printf("%f qps\n", num / d.Seconds())
    writeMB := num * valueSize / 1e6
    fmt.Printf("%f MB/s\n", writeMB / d.Seconds())
    fmt.Printf("%f micros/op\n", d.Seconds() * 1e6 / num)
}

func main() {
    flag.Parse()

    op := bitcask.NewOptions()
    var err error
    bc, err = bitcask.Open(dbPath, op)
    if err != nil {
        log.Fatal(err)
        return
    }
    defer bc.Close()

    rand.Seek(time.Now().UnixNano())
    BenchRandomSet(num)
    BenchRandomGet(num)
}


