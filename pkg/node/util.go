package node

import (
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/common/storj"
	"time"
)

func measure(f func() error) (time.Duration, error) {
	start := time.Now()
	err := f()
	return time.Since(start), err
}

func forEachNode(file string, cb func(node storj.NodeURL, values map[string]string) error) error {
	input, err := os.Open(file)
	if err != nil {
		return errs.Wrap(err)
	}
	defer input.Close()
	nodes := csv.NewReader(input)
	headers := map[string]int{}
	for {
		record, err := nodes.Read()
		if errors.Is(io.EOF, err) {
			break
		}
		if err != nil {
			return err
		}
		if len(headers) == 0 {
			for i, r := range record {
				headers[r] = i
			}
			continue
		}

		idBytes, err := hex.DecodeString(record[headers["id"]])
		if err != nil {
			return errs.Wrap(err)
		}

		nodeID, err := storj.NodeIDFromBytes(idBytes)
		if err != nil {
			return errs.Wrap(err)
		}

		noise := storj.NoiseInfo{}
		if record[headers["noise_public_key"]] != "" {
			noise.Proto = storj.NoiseProto_IK_25519_ChaChaPoly_BLAKE2b
			decoded, err := hex.DecodeString(record[headers["noise_public_key"]])
			if err != nil {
				panic(err)
			}
			noise.PublicKey = string(decoded)
		}
		nodeURL := storj.NodeURL{
			ID:        nodeID,
			Address:   record[headers["address"]],
			NoiseInfo: noise,
		}
		r := map[string]string{}
		for k, v := range headers {
			if v < len(record) {
				r[k] = record[v]
			}
		}

		err = cb(nodeURL, r)
		if err != nil {
			fmt.Println(nodeID, err)
		}
	}
	return nil
}
