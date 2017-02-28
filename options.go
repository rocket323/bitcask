package bitcask

type Options struct {
    maxFileSize         int64
}

func NewOptions() *Options {
    op := &Options {
        maxFileSize: 10 * 1024 * 1024,
    }
    return op
}

