package s3_log

import "context"

type Record struct {
	Offset uint64
	Data   []byte
}

type WAL interface {
	Append(ctx context.Context, data []byte) (uint64, error)
	Read(ctx context.Context, offset uint64) (Record, error)
}
