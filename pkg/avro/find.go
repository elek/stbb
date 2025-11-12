package avro

import (
	"bufio"
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/api/iterator"
)

type Find struct {
	Bucket   string `arg:"" help:"GCS bucket name"`
	Prefix   string `arg:"" help:"Prefix path in the bucket to search Avro files"`
	KeyField string `arg:"" help:"Primary key field name to search for"`
	KeyValue string `arg:"" help:"Value of the primary key to search for"`
}

func (f Find) Run() error {
	ctx := context.Background()

	// Search for the record
	record, err := SearchAvroRecord(ctx, f.Bucket, f.Prefix, f.KeyField, f.KeyValue)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Print the record
	PrintRecord(record)
	return nil
}

// SearchAvroRecord searches for a record with the given primary key across all Avro files
func SearchAvroRecord(ctx context.Context, bucketName, prefix, keyField string, keyValue interface{}) (map[string]interface{}, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)

	// Iterate through all objects in the bucket with the given prefix
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating objects: %w", err)
		}

		fmt.Printf("Searching in: %s\n", attrs.Name)

		// Search in this file
		record, err := searchInFile(ctx, bucket, attrs.Name, keyField, keyValue)
		if err != nil {
			log.Printf("Error searching file %s: %v", attrs.Name, err)
			continue
		}
		if record != nil {
			fmt.Printf("\n✓ Found in: %s\n", attrs.Name)
			return record, nil
		}
	}

	return nil, fmt.Errorf("record with %s=%v not found", keyField, keyValue)
}

// searchInFile searches for a record in a single Avro file
func searchInFile(ctx context.Context, bucket *storage.BucketHandle, objectName, keyField string, keyValue interface{}) (map[string]interface{}, error) {
	fmt.Println("Searching in", objectName)
	obj := bucket.Object(objectName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read object: %w", err)
	}
	defer reader.Close()
	br := bufio.NewReader(reader)
	ocfReader, err := goavro.NewOCFReader(br)
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader: %w", err)
	}

	// Iterate through records
	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		if err != nil {
			log.Printf("Error reading record: %v", err)
			continue
		}
		// Check if this is a map
		recordMap, ok := record.(map[string]interface{})
		if !ok {
			continue
		}
		if recordMap[keyField] == nil {
			continue
		}
		fmt.Println(recordMap[keyField])
		// Check if the key matches
		if recordMap[keyField] == keyValue {
			return recordMap, nil
		}
	}

	return nil, nil
}

func PrintRecord(record map[string]interface{}) {
	fmt.Println("\n=== Record Details ===")
	for key, value := range record {
		fmt.Printf("%s: %v\n", key, value)
	}
	fmt.Println("=====================")
}
