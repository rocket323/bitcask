package bitcask

type Options struct {
    maxFileSize         int64
    maxOpenFiles        uint32
}

func NewOptions() *Options {
    op := &Options {
        maxFileSize: 10 * 1024 * 1024,
    }
    return op
}

func (o *Options) SetMaxFileSize(n int64) {
    o.maxFileSize = n
}

func (o *Options) SetMaxOpenFiles(n int32) {
    o.maxOpenFiles = n
}

