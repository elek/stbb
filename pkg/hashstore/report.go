package hashstore

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"storj.io/common/memory"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Report struct {
	WithHashtable
	JSON bool `help:"Output in JSON format"`
}

func (i *Report) Run() error {
	ctx := context.Background()

	report := HashstoreReport{}

	paths := []string{i.WithHashtable.Path}
	if _, err := os.Stat(filepath.Join(i.Path, "s0")); err == nil {
		paths = []string{filepath.Join(i.Path, "s0", "meta"), filepath.Join(i.Path, "s1", "meta")}
		if !i.JSON {
			fmt.Println("Checking both hashtable:", paths)
		} else {
			report.Table = "both"
		}
	} else {
		report.Table = i.Path
	}

	today := timeToDateDown(time.Now())
	ttlHistogram := NewTimeHistogram()
	trashHistogram := NewTimeHistogram()

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
					trashHistogram.Increment(expRel, int(record.Length))
				} else {
					ttlHistogram.Increment(expRel, int(record.Length))
				}
			} else {
				report.Sum.NonTTL.Count++
				report.Sum.NonTTL.Size += int(record.Length)
			}
			report.Stat.Count++
			report.Stat.Size += int(record.Length)
			return true, nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Convert histograms to report format
	for day, count := range ttlHistogram.count {
		report.TTL = append(report.TTL, HistogramItem{
			Day:   day,
			Count: count,
			Size:  ttlHistogram.size[day],
		})
		report.Sum.TTL.Count += count
		report.Sum.TTL.Size += ttlHistogram.size[day]
	}

	for day, count := range trashHistogram.count {
		report.Trash = append(report.Trash, HistogramItem{
			Day:   day,
			Count: count,
			Size:  trashHistogram.size[day],
		})
		report.Sum.Trash.Count += count
		report.Sum.Trash.Size += trashHistogram.size[day]
	}

	sort.Slice(report.TTL, func(i, j int) bool {
		return report.TTL[i].Day < report.TTL[j].Day
	})

	sort.Slice(report.Trash, func(i, j int) bool {
		return report.Trash[i].Day < report.Trash[j].Day
	})

	// Print report
	if i.JSON {
		// Print JSON format
		jsonData, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Println(string(jsonData))
	} else {
		// Print human-readable format
		fmt.Println("pieces", report.Stat.Count)
		fmt.Println("size", report.Stat.Size)
		if report.Stat.Count > 0 {
			fmt.Println("average size", report.Stat.Size/report.Stat.Count)
		}
		fmt.Println()
		fmt.Println("no-ttl", report.Sum.NonTTL.Count)
		fmt.Println("ttl", report.Sum.TTL.Count)
		fmt.Println("trash", report.Sum.Trash.Count)
		fmt.Println()
		fmt.Println("TTL PER DAY")
		ttlHistogram.Print(-50, 50)
		fmt.Println("TRASH PER DAY")
		trashHistogram.Print(-50, 10)
	}

	return nil
}

type HashstoreReport struct {
	Table string
	Stat  PieceStat
	Sum   struct {
		NonTTL PieceStat
		TTL    PieceStat
		Trash  PieceStat
	}
	Trash []HistogramItem
	TTL   []HistogramItem
}

type PieceStat struct {
	Count int
	Size  int
}

type HistogramItem struct {
	Day   int
	Count int
	Size  int
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
