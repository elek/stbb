package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"math"
	"os"
	"path/filepath"
	"storj.io/common/memory"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Report struct {
	WithHashtable
}

func (i *Report) Run() error {
	ctx := context.Background()

	paths := []string{i.WithHashtable.Path}
	if _, err := os.Stat(filepath.Join(i.Path, "s0")); err == nil {
		paths = []string{filepath.Join(i.Path, "s0", "meta"), filepath.Join(i.Path, "s1", "meta")}
		fmt.Println("Checking both hashtable:", paths)
	}

	nonTTL := 0
	today := timeToDateDown(time.Now())
	ttlHistogram := NewTimeHistogram()
	trashHistorgram := NewTimeHistogram()
	all := 0
	size := 0

	for _, p := range paths {
		i.WithHashtable.Path = p
		hashtbl, close, err := i.WithHashtable.Open(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer close()

		err = hashtbl.Range(ctx, func(ctx2 context.Context, record hashstore.Record) (bool, error) {
			if record.Expires.Set() {
				expRel := int(record.Expires.Time()) - int(today)
				if record.Expires.Trash() {
					trashHistorgram.Increment(expRel, int(record.Length))
				} else {
					ttlHistogram.Increment(expRel, int(record.Length))
				}
			} else {
				nonTTL++
			}
			all++
			size += int(record.Length)
			return true, nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	fmt.Println("pieces", all)
	fmt.Println("size", size)
	fmt.Println("average size", size/all)
	fmt.Println()
	fmt.Println("no-ttl", nonTTL)
	fmt.Println("ttl", ttlHistogram.Count())
	fmt.Println("trash", trashHistorgram.Count())
	fmt.Println()
	fmt.Println("TTL PER DAY")
	ttlHistogram.Print(-50, 50)
	fmt.Println("TRASH PER DAY")
	trashHistorgram.Print(-50, 10)

	return nil
}

type TimeHistogram struct {
	count map[int]int
	size  map[int]int
}

func NewTimeHistogram() *TimeHistogram {
	return &TimeHistogram{
		count: map[int]int{},
		size:  map[int]int{},
	}
}

func (t *TimeHistogram) Increment(idx int, size int) {
	t.count[idx]++
	t.size[idx] += size
}

func (t *TimeHistogram) Count() (res int) {
	for _, v := range t.count {
		res += v
	}
	return res
}

func (t *TimeHistogram) Print(minLimit int, maxLimit int) {
	if len(t.count) == 0 {
		return
	}
	min := math.MaxInt
	max := 0
	underLimitCounter := 0
	overLimitCounter := 0
	for k := range t.count {
		if k < minLimit {
			underLimitCounter += t.count[k]
			continue
		}
		if k > maxLimit {
			overLimitCounter += t.count[k]
			continue
		}
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
	}
	if underLimitCounter > 0 {
		fmt.Println("  EARLIER", underLimitCounter)
	}
	for i := -50; i <= max; i++ {
		if _, found := t.count[i]; !found {
			continue
		}
		fmt.Printf("  TODAY+%d: %d records (%s bytes)\n", i, t.count[i], memory.Size(t.size[i]).String())
	}
	if overLimitCounter > 0 {
		fmt.Println("  LATER", overLimitCounter)
	}
}
