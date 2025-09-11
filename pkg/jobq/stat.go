package jobq

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/storj"
	"storj.io/storj/satellite/jobq"
)

type Stat struct {
	Server        string `required:"true"`
	Identity      string `required:"true"`
	WithHistogram bool   `help:"Include raw histogram data in the output (all records)."`
	Histogram     string `help:"Include histogram data in the output (healthy,retrievable,oop)."`
	Placement     *int
	Retry         bool
}

func (s *Stat) Run() error {
	ctx := context.Background()

	cfg := identity.Config{
		CertPath: filepath.Join(s.Identity, "identity.cert"),
		KeyPath:  filepath.Join(s.Identity, "identity.key"),
	}
	identity, err := cfg.Load()
	if err != nil {
		return errors.WithStack(err)
	}
	opts := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}
	tlsOpts, err := tlsopts.NewOptions(identity, opts, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	serverNodeURL, err := storj.ParseNodeURL(s.Server)
	if err != nil {
		return errors.WithStack(err)
	}

	dialer := jobq.NewDialer(tlsOpts)

	conn, err := dialer.DialNodeURL(ctx, serverNodeURL)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	client := jobq.WrapConn(conn)
	defer client.Close()

	var stats []jobq.QueueStat
	if s.Placement == nil {
		results, err := client.StatAll(ctx, s.WithHistogram || s.Histogram != "")
		if err != nil {
			return errors.WithStack(err)
		}
		stats = append(stats, results...)
	} else {
		result, err := client.Stat(ctx, storj.PlacementConstraint(*s.Placement), s.WithHistogram || s.Histogram != "")
		if err != nil {
			return errors.WithStack(err)
		}
		stats = append(stats, result)
	}

	for ix, stat := range stats {
		if s.Retry && ix%2 == 0 {
			continue
		}
		if !s.Retry && ix%2 == 1 {
			continue
		}
		fmt.Println("Placement", stat.Placement)
		fmt.Println("Count", stat.Count)
		fmt.Println("MaxSegmentHealth", stat.MaxSegmentHealth)
		fmt.Println("MinSegmentHealth", stat.MinSegmentHealth)
		fmt.Println()
		if s.WithHistogram {
			fmt.Println("Histogram:")
			for _, bucket := range stat.Histogram {
				fmt.Printf("  missing: %d, retrivbl: %d, oop: %d (%s/%d) > %d\n", bucket.NumNormalizedHealthy, bucket.NumNormalizedRetrievable, bucket.NumOutOfPlacement, bucket.Exemplar.StreamID, bucket.Exemplar.Position, bucket.Count)
			}
			fmt.Println()
		}
		if s.Histogram != "" {
			buckets := make(map[int64]*histBucket)
			minv := int64(-1)
			maxv := int64(-1)
			for _, bucket := range stat.Histogram {
				key := bucket.NumNormalizedHealthy
				if minv == -1 || minv > key {
					minv = key
				}
				if maxv == -1 || maxv < key {
					maxv = key
				}
				if _, found := buckets[key]; !found {
					buckets[key] = &histBucket{
						count:     0,
						exemplars: []string{},
					}
				}
				buckets[key].count += bucket.Count
				buckets[key].exemplars = append(buckets[key].exemplars, fmt.Sprintf("%s/%d", bucket.Exemplar.StreamID, bucket.Exemplar.Position))
			}
			for i := minv; i <= maxv; i++ {
				if _, found := buckets[i]; !found {
					fmt.Println("  ", i)
					continue
				}
				fmt.Println("  ", i, buckets[i].count, buckets[i].exemplars[0], len(buckets[i].exemplars))
			}
		}
	}
	return nil
}

type histBucket struct {
	count     int64
	exemplars []string
}
