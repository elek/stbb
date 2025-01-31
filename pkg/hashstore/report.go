package hashstore

import (
	"fmt"
	"github.com/pkg/errors"
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
	hashtbl, err := hashstore.OpenHashtbl(o)
	if err != nil {
		return errors.WithStack(err)
	}
	defer hashtbl.Close()

	nonTTL := 0
	ttl := 0
	ttlFar := 0
	trash := 0
	today := timeToDateDown(time.Now())
	var ttlPrev, ttlNext []int
	hashtbl.Range(func(record hashstore.Record, err error) bool {
		if record.Expires.Set() {
			expRel := int(record.Expires.Time() - today)
			if expRel > 100 || expRel < -100 {
				ttlFar++
			} else if expRel < 0 {
				expRel = -expRel
				if len(ttlPrev) <= expRel {
					ttlPrev = append(ttlPrev, make([]int, expRel-len(ttlPrev)+1)...)
				}
				ttlPrev[expRel]++
			} else {
				if len(ttlNext) <= expRel {
					ttlNext = append(ttlNext, make([]int, expRel-len(ttlNext)+1)...)
				}
				ttlNext[expRel]++
			}
			if record.Expires.Trash() {
				trash++
			}
			ttl++
		} else {
			nonTTL++
		}

		return true
	})
	fmt.Println("no-ttl", nonTTL)
	fmt.Println("ttl", ttl)
	fmt.Println("ttlFar", ttlFar)
	fmt.Println("trash", trash)
	fmt.Println("TTL PREV")
	for i := 0; i < len(ttlPrev); i++ {
		fmt.Println(dateToTime(uint32(i)+today).Format(time.RFC3339), ttlPrev[i])
	}
	fmt.Println("TTL NEXT")
	for i := 0; i < len(ttlNext); i++ {
		fmt.Println(dateToTime(uint32(i)+today).Format(time.RFC3339), ttlNext[i])
	}
	return nil
}
