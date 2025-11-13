package avro

import (
	"bufio"
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
)

type List struct {
	WithAvroFiles
	KeyField string `arg:"" help:"Primary key field name to search for"`
}

func (f *List) Run() error {
	ctx := context.Background()

	err := f.WithAvroFiles.ForEach(func(bucket *storage.BucketHandle, name string) error {
		obj := bucket.Object(name)
		reader, err := obj.NewReader(ctx)
		if err != nil {
			return fmt.Errorf("failed to read object: %w", err)
		}
		defer reader.Close()
		br := bufio.NewReader(reader)
		ocfReader, err := goavro.NewOCFReader(br)
		if err != nil {
			return fmt.Errorf("failed to create OCF reader: %w", err)
		}

		// Iterate through records
		ocfReader.Scan()
		record, err := ocfReader.Read()
		if err != nil {
			return errors.WithStack(err)

		}
		// Check if this is a map
		recordMap := record.(map[string]interface{})
		fmt.Println(bucket.BucketName(), name, display(recordMap[f.KeyField]))

		return nil
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	return nil
}

func display(i interface{}) any {
	switch k := i.(type) {
	case []byte:
		return fmt.Sprintf("%x", k)
	default:
		return fmt.Sprintf("%v", i)
	}
}
