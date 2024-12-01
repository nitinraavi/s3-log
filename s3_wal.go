package s3_log

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3WAL struct {
	client     *s3.Client
	bucketName string
	prefix     string
	length     uint64
}

func NewS3WAL(client *s3.Client, bucketName, prefix string) *S3WAL {
	return &S3WAL{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
		length:     0,
	}
}

func (w *S3WAL) Append(ctx context.Context, data []byte) (uint64, error) {
	nextOffset := w.length + 1

	input := &s3.PutObjectInput{
		Bucket:      aws.String(w.bucketName),
		Key:         aws.String(fmt.Sprintf("%020d", nextOffset)),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	}

	if _, err := w.client.PutObject(ctx, input); err != nil {
		return 0, fmt.Errorf("failed to put object to S3: %w", err)
	}
	w.length = nextOffset
	return nextOffset, nil
}
