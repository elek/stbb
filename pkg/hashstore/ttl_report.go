package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/storj/storagenode/hashstore"
)

type TTLReport struct {
	Hashstore string `arg:""`
}

func (l *TTLReport) Run() error {
	o, err := os.Open(l.Hashstore)
	if err != nil {
		return errors.WithStack(err)
	}

	defer o.Close()
	ctx := context.Background()
	hashtbl, err := hashstore.OpenHashtbl(ctx, o)
	if err != nil {
		return errors.WithStack(err)
	}
	defer hashtbl.Close()

	// logid --> TTL --> count
	expired := make(map[uint64]map[hashstore.Expiration]int)

	err = hashtbl.Range(ctx, func(ctx context.Context, rec hashstore.Record) (bool, error) {
		if _, found := expired[rec.Log]; !found {
			expired[rec.Log] = make(map[hashstore.Expiration]int)
		}

		expired[rec.Log][rec.Expires]++
		return true, nil
	})

	if err != nil {
		return err
	}

	for logid, ttls := range expired {
		fmt.Println("LOG", logid)
		for expires, count := range ttls {
			fmt.Println("   ", expires.Trash(), expires.Time(), count)
		}

	}
	return nil
}
