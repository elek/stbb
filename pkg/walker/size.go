package walker

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/blobstore/filestore"
	"storj.io/storj/storagenode/blobstore/statcache"
	"storj.io/storj/storagenode/pieces"
	"time"
)

type Size struct {
	Dir       string       `arg:""`
	Satellite storj.NodeID `default:"12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S"`
	Cache     string
}

func (s Size) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	piecesDir, err := filestore.OpenDir(log, s.Dir, time.Now())
	if err != nil {
		return errors.WithStack(err)
	}

	blobStore := filestore.New(log, piecesDir, filestore.Config{})
	defer blobStore.Close()

	switch s.Cache {
	case "file":
		blobStore = statcache.NewCachedStatBlobStore(statcache.NewFileCache("/tmp/statcache"), blobStore)
	case "lie":
		blobStore = statcache.NewCachedStatBlobStore(statcache.Lie{}, blobStore)
	case "badger":
		cache, err := statcache.NewBadgerCache(filepath.Join(filepath.Dir(s.Dir), "filestat"))
		if err != nil {
			return errors.WithStack(err)
		}
		defer cache.Close()

		blobStore = statcache.NewCachedStatBlobStore(cache, blobStore)
	default:
		panic("Unknown cache: " + s.Cache)
	}

	fw := pieces.NewFileWalker(log, blobStore, nil, nil)

	var satPiecesTotal, satPiecesContentSize int64

	last := time.Now()
	counter := 0
	lastCounter := 0
	err = fw.WalkSatellitePieces(context.Background(), s.Satellite, "", func(access pieces.StoredPieceAccess) error {
		pieceTotal, pieceContentSize, err := access.Size(context.Background())
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		satPiecesTotal += pieceTotal
		satPiecesContentSize += pieceContentSize
		counter++
		lastCounter++
		if counter%10000 == 0 && time.Since(last) > 5*time.Minute {
			fmt.Printf("%d %v/sec\n", counter, float64(lastCounter)/time.Since(last).Seconds())
			lastCounter = 0
			last = time.Now()
		}
		return nil
	})

	fmt.Println(counter, satPiecesTotal, satPiecesContentSize)
	return nil
}
