package hashstore

import (
	"github.com/elek/stbb/pkg/load"
	"time"
)

type Generate struct {
	load.PieceIDStream
	Dir          string        `help:"directory of the store (not the db!)" default:"/tmp/store"`
	Samples      int           `help:"number of pieces to be written" default:"1"`
	Size         int64         `help:"size of the pieces to be written" default:"100000"`
	TTL          time.Duration `help:"TTL to be used"`
	TTLModulo    int           `help:"modulo for TTL. Only the selected pieces with this modulo will be TTLed" default:"1"`
	MinTableSize uint64        `help:"minimum table size for the store" default:"20"`
	MetaPath     string        `help:"path to the meta file" default:""`
}

func (b Generate) Run() error {
	//log, err := zap.NewDevelopment()
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//opts := []any{hashstore.MinTableSize(b.MinTableSize)}
	//if b.MetaPath != "" {
	//	opts = append(opts, hashstore.MetaDirPath(b.MetaPath))
	//}
	//store, err := hashstore.NewStore(b.LogDir, log, opts...)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//defer store.Close()
	//ctx := context.Background()
	//
	//data, err := io.ReadAll(io.LimitReader(crand.Reader, b.Size))
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//
	//for i := 0; i < b.Samples; i++ {
	//	var expiresTime time.Time
	//	if i%b.TTLModulo == 0 && b.TTL != 0 {
	//		expiresTime = time.Now().Add(b.TTL)
	//	}
	//	err = b.writeOne(ctx, store, data, expiresTime)
	//	if err != nil {
	//		return errors.WithStack(err)
	//	}
	//
	//}
	return nil
}

//func (b Generate) writeOne(ctx context.Context, store *hashstore.Store, data []byte, expiresTime time.Time) (err error) {
//	defer mon.Task()(&ctx)(&err)
//	create, err := store.Create(ctx, hashstore.Key(b.NextPieceID()), expiresTime)
//	if err != nil {
//		return errors.WithStack(err)
//	}
//	defer create.Close()
//	_, err = create.Write(data)
//	if err != nil {
//		return errors.WithStack(err)
//	}
//	return err
//}
