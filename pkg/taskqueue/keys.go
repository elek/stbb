package taskqueue

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// Keys lists all Redis stream keys and their lengths.
type Keys struct {
	Address string `help:"Redis URL for task queue" default:"redis://localhost:6379"`
}

func (k *Keys) Run() error {
	ctx := context.Background()

	redisOpts, err := redis.ParseURL(k.Address)
	if err != nil {
		return errors.WithStack(err)
	}
	redisOpts.ReadTimeout = 5 * time.Minute
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	type streamInfo struct {
		key    string
		length int64
	}

	var streams []streamInfo
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			return errors.WithStack(err)
		}
		for _, key := range keys {
			t, err := rdb.Type(ctx, key).Result()
			if err != nil {
				return errors.WithStack(err)
			}
			if t != "stream" {
				continue
			}
			length, err := rdb.XLen(ctx, key).Result()
			if err != nil {
				return errors.WithStack(err)
			}
			streams = append(streams, streamInfo{key: key, length: length})
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	sort.Slice(streams, func(i, j int) bool {
		return streams[i].length > streams[j].length
	})

	for _, s := range streams {
		fmt.Printf("%8d %s\n", s.length, s.key)
	}
	return nil
}
