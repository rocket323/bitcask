package bitcask

type Options struct {
    maxFileSize         int64
    maxOpenFiles        uint32
    bufferSize          int64
}

func NewOptions() *Options {
    op := &Options {
        maxFileSize: 100 * 1024 * 1024,
        maxOpenFiles: 4096,
        bufferSize: 10 * 1024 * 1024,
    }
    return op
}

func (o *Options) SetMaxFileSize(n int64) {
    o.maxFileSize = n
}

func (o *Options) SetMaxOpenFiles(n int32) {
    o.maxOpenFiles = uint32(n)
}

func (o *Options) SetBufferSize(n int64) {
    o.bufferSize = n
}

