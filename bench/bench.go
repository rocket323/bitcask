package main

import (
    "fmt"
    "flag"
    "math/rand"
    "time"
    "log"
    "github.com/rocket323/bitcask"
)

var (
    bc *bitcask.BitCask
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
        key := fmt.Sprintf("%09d", rand.Int() % num)
        err := bc.Set(key, value)
        if err != nil {
            log.Println(err)
            panic(err)
        }
    }
    end := time.Now()
    d := end.Sub(start)
    fmt.Printf("========\nset finish in %.2f seconds\n", d.Seconds())
    fmt.Printf("%.2f qps\n", float64(num) / d.Seconds())
    writeMB := float64(num * valueSize) / 1e6
    fmt.Printf("%.2f MB/s\n", writeMB / d.Seconds())
    fmt.Printf("%.2f micros/op\n", d.Seconds() * 1e6 / float64(num))
}

func BenchRandomGet(num int) {
    start := time.Now()
    var found = 0
    for i := 0; i < num; i++ {
        key := fmt.Sprintf("%09d", rand.Int() % num)
        _, err := bc.Get(key)
        if err != nil && err != bitcask.ErrNotFound {
            log.Println(err)
            panic(err)
        }
        if err == nil {
            found++
        }
    }
    end := time.Now()
    d := end.Sub(start)
    fmt.Printf("========\nget finish in %.2f seconds\n", d.Seconds())
    fmt.Printf("found %d out of %d\n", found, num)
    fmt.Printf("%.2f qps\n", float64(num) / d.Seconds())
    writeMB := float64(num * valueSize) / 1e6
    fmt.Printf("%.2f MB/s\n", writeMB / d.Seconds())
    fmt.Printf("%.2f micros/op\n", d.Seconds() * 1e6 / float64(num))
}

func main() {
    flag.Parse()
    log.SetFlags(log.Lshortfile | log.LstdFlags)

    op := bitcask.NewOptions()
    var err error
    bc, err = bitcask.Open(dbPath, op)
    if err != nil {
        log.Println(err)
        return
    }
    defer bc.Close()

    rand.Seed(time.Now().UnixNano())
    BenchRandomSet(num)
    BenchRandomGet(num)
}

