package s3_log

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3WAL struct {
	client     *s3.Client
	bucketName string
	prefix     uint64
	length     uint64
	mu         sync.Mutex
}

func NewS3WAL(client *s3.Client, bucketName string, prefix uint64) *S3WAL {
	return &S3WAL{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
		length:     0,
	}
}

// func (w *S3WAL) getObjectKey(offset uint64) string {
// 	fmt.Println(w.prefix + "/" + fmt.Sprintf("%020d", offset))
// 	return w.prefix + "/" + fmt.Sprintf("%020d", offset)

// }

func (w *S3WAL) getObjectKey(offset uint64) (string, uint64) {
	prefix := (offset - 1) / 64             // Calculate the prefix based on 64 records per group
	groupOffset := 64 - ((offset - 1) % 64) // Reverse numbering: starts at 64 and ends at 1
	// fmt.Printf("/record/%03d/%010d.data", prefix, groupOffset)
	w.prefix = prefix
	return fmt.Sprintf("/record/%03d/%010d.data", prefix, groupOffset), groupOffset

}

// func (w *S3WAL) getOffsetFromKey(key string) (uint64, error) {
// 	// skip the `w.prefix` and "/"
// 	numStr := key[len(w.prefix)+1:]
// 	return strconv.ParseUint(numStr, 10, 64)
// }

func (w *S3WAL) getOffsetFromKey(key string) (uint64, error) {
	// Extract the prefix and groupOffset from the key
	var prefix, groupOffset uint64
	_, err := fmt.Sscanf(key, "/record/%03d/%010d.data", &prefix, &groupOffset)
	if err != nil {
		return 0, fmt.Errorf("invalid key format: %s", key)
	}

	// Calculate the offset using the reverse numbering logic
	offset := (prefix * 64) + (64 - groupOffset + 1)
	return offset, nil
}

func calculateChecksum(buf *bytes.Buffer) [32]byte {
	return sha256.Sum256(buf.Bytes())
}

func validateChecksum(data []byte) bool {
	var storedChecksum [32]byte
	copy(storedChecksum[:], data[len(data)-32:])
	recordData := data[:len(data)-32]
	return storedChecksum == calculateChecksum(bytes.NewBuffer(recordData))
}

func prepareBody(offset uint64, data []byte) ([]byte, error) {
	// 8 bytes for offset, len(data) bytes for data, 32 bytes for checksum
	bufferLen := 8 + len(data) + 32
	buf := bytes.NewBuffer(make([]byte, 0, bufferLen))
	if err := binary.Write(buf, binary.BigEndian, offset); err != nil {
		return nil, err
	}
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	checksum := calculateChecksum(buf)
	_, err := buf.Write(checksum[:])
	return buf.Bytes(), err
}

func (w *S3WAL) Append(ctx context.Context, data []byte) (uint64, error) {
	w.mu.Lock() // Acquire the lock
	defer w.mu.Unlock()
	nextOffset := w.length + 1

	buf, err := prepareBody(nextOffset, data)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare object body: %w", err)
	}
	key, _ := w.getObjectKey(nextOffset)
	input := &s3.PutObjectInput{
		Bucket:      aws.String(w.bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf),
		IfNoneMatch: aws.String("*"),
	}

	if _, err = w.client.PutObject(ctx, input); err != nil {
		return 0, fmt.Errorf("failed to put object to S3: %w", err)
	}

	//  Check if we should write a checkpoint (when groupOffset resets to 64)
	if w.prefix != 0 {
		fmt.Printf("\nPersisted prefix: %d\n", w.prefix) // Access the persisted prefix
		// This means we are starting a new group
		checkpointKey := fmt.Sprintf("/checkpoint/%03d.data", w.prefix)

		// Create the checkpoint data
		checkpointData := []byte(fmt.Sprintf("Checkpoint for group %03d", w.prefix))

		checkpointInput := &s3.PutObjectInput{
			Bucket: aws.String(w.bucketName),
			Key:    aws.String(checkpointKey),
			Body:   bytes.NewReader(checkpointData),
		}

		if _, err = w.client.PutObject(ctx, checkpointInput); err != nil {
			return 0, fmt.Errorf("failed to write checkpoint to S3: %w", err)
		}
	}
	w.length = nextOffset
	return nextOffset, nil
}

func (w *S3WAL) Read(ctx context.Context, offset uint64) (Record, error) {
	key, _ := w.getObjectKey(offset)
	input := &s3.GetObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(key),
	}

	result, err := w.client.GetObject(ctx, input)
	if err != nil {
		return Record{}, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return Record{}, fmt.Errorf("failed to read object body: %w", err)
	}
	if len(data) < 40 {
		return Record{}, fmt.Errorf("invalid record: data too short")
	}

	var storedOffset uint64
	if err = binary.Read(bytes.NewReader(data[:8]), binary.BigEndian, &storedOffset); err != nil {
		return Record{}, err
	}
	if storedOffset != offset {
		return Record{}, fmt.Errorf("offset mismatch: expected %d, got %d", offset, storedOffset)
	}
	if !validateChecksum(data) {
		return Record{}, fmt.Errorf("checksum mismatch")
	}
	return Record{
		Offset: storedOffset,
		Data:   data[8 : len(data)-32],
	}, nil
}

func (w *S3WAL) LastRecord(ctx context.Context) (Record, error) {
	prefixString := fmt.Sprintf("/record/%03d/", w.prefix)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucketName),
		Prefix: aws.String(prefixString + "/"),
	}
	paginator := s3.NewListObjectsV2Paginator(w.client, input)

	var maxOffset uint64 = 0
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return Record{}, fmt.Errorf("failed to list objects from S3: %w", err)
		}
		for _, obj := range output.Contents {
			key := *obj.Key
			offset, err := w.getOffsetFromKey(key)
			if err != nil {
				return Record{}, fmt.Errorf("failed to parse offset from key: %w", err)
			}
			if offset > maxOffset {
				maxOffset = offset
			}
		}
	}
	if maxOffset == 0 {
		return Record{}, fmt.Errorf("WAL is empty")
	}
	w.length = maxOffset
	return w.Read(ctx, maxOffset)
}
