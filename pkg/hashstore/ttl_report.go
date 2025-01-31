package hashstore

import (
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
	hashtbl, err := hashstore.OpenHashtbl(o)
	if err != nil {
		return errors.WithStack(err)
	}
	defer hashtbl.Close()

	// logid --> TTL --> count
	expired := make(map[uint64]map[hashstore.Expiration]int)

	hashtbl.Range(func(rec hashstore.Record, err error) bool {
		if _, found := expired[rec.Log]; !found {
			expired[rec.Log] = make(map[hashstore.Expiration]int)
		}

		expired[rec.Log][rec.Expires]++
		return true
	})

	for logid, ttls := range expired {
		fmt.Println("LOG", logid)
		for expires, count := range ttls {
			fmt.Println("   ", expires.Trash(), expires.Time(), count)
		}

	}
	return nil
}
