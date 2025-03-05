package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"math"
	"os"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Report struct {
	Path string `arg:""`
}

func (i *Report) Run() error {
	o, err := os.Open(i.Path)
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

	nonTTL := 0
	today := timeToDateDown(time.Now())
	ttlHistogram := NewTimeHistogram()
	trashHistorgram := NewTimeHistogram()
	err = hashtbl.Range(ctx, func(ctx2 context.Context, record hashstore.Record) (bool, error) {
		if record.Expires.Set() {
			expRel := int(record.Expires.Time() - today)
			if record.Expires.Trash() {
				trashHistorgram.Increment(expRel)
			} else {
				ttlHistogram.Increment(expRel)
			}
		} else {
			nonTTL++
		}

		return true, nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
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
	values map[int]int
}

func NewTimeHistogram() *TimeHistogram {
	return &TimeHistogram{values: map[int]int{}}
}

func (t *TimeHistogram) Increment(idx int) {
	t.values[idx]++
}

func (t *TimeHistogram) Count() (res int) {
	for _, v := range t.values {
		res += v
	}
	return res
}

func (t *TimeHistogram) Print(minLimit int, maxLimit int) {
	if len(t.values) == 0 {
		return
	}
	min := math.MaxInt
	max := 0
	underLimitCounter := 0
	overLimitCounter := 0
	for k := range t.values {
		if k < minLimit {
			underLimitCounter += t.values[k]
			continue
		}
		if k > maxLimit {
			overLimitCounter += t.values[k]
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
	for i := min; i <= max; i++ {
		fmt.Printf("  TODAY+%d: %d records\n", i, t.values[i])
	}
	if overLimitCounter > 0 {
		fmt.Println("  LATER", overLimitCounter)
	}
}
