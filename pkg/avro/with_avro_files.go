package avro

import (
	"context"
	"errors"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type WithAvroFiles struct {
	Bucket string `arg:"" help:"GCS bucket name"`
	Prefix string `arg:"" help:"Prefix path in the bucket to search Avro files"`
}

func (f *WithAvroFiles) ForEach(cb func(bucket *storage.BucketHandle, name string) error) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(f.Bucket)
	query := &storage.Query{Prefix: f.Prefix}
	it := bucket.Objects(ctx, query)

	// Iterate through all objects in the bucket with the given prefix
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			fmt.Errorf("error iterating objects: %w", err)
		}

		fmt.Printf("Searching in: %s\n", attrs.Name)

		err = cb(bucket, attrs.Name)
		if err != nil {
			log.Printf("Error processing file %s: %v", attrs.Name, err)
			continue
		}
	}

	return nil
}
