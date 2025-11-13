package avro

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/linkedin/goavro/v2"
)

type Find struct {
	WithAvroFiles
	KeyField string `arg:"" help:"Primary key field name to search for"`
	KeyValue string `arg:"" help:"Value of the primary key to search for"`
	Debug    bool   `help:"Enable debug mode"`
}

func (f *Find) Run() error {
	ctx := context.Background()

	var value any
	value = f.KeyValue
	raw, err := hex.DecodeString(f.KeyValue)
	if err == nil {
		fmt.Println("HEX value using it as a []byte")
		value = raw
	}

	// Search for the record
	err = f.WithAvroFiles.ForEach(func(bucket *storage.BucketHandle, name string) error {
		record, err := f.searchInFile(ctx, bucket, name, f.KeyField, value)
		if err != nil {
			return err
		}
		if record != nil {
			PrintRecord("", record)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	return nil
}

// searchInFile searches for a record in a single Avro file
func (f *Find) searchInFile(ctx context.Context, bucket *storage.BucketHandle, objectName, keyField string, keyValue interface{}) (map[string]interface{}, error) {
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
		if f.Debug {
			fmt.Println(hex.EncodeToString(recordMap[keyField].([]byte)))
		}
		if bytes.Equal(recordMap[keyField].([]byte), keyValue.([]byte)) {
			return recordMap, nil
		}
	}

	return nil, nil
}

func PrintRecord(prefix string, record map[string]interface{}) {
	for key, value := range record {
		if m, ok := value.(map[string]interface{}); ok {
			for k, v := range m {
				switch k {
				case "string", "long":
					fmt.Printf("%s %s: %v\n", prefix, key, v)
				case "bytes":
					fmt.Printf("%s %s: %x\n", prefix, key, v)
				default:
					fmt.Printf("%s %s: %v (%s)\n", prefix, key, v, k)
				}
			}
			continue
		}

		format := "%v"
		if _, ok := value.([]byte); ok {
			format = "%x"
		}

		fmt.Printf("%s %s: "+format+"\n", prefix, key, value)
	}
}
