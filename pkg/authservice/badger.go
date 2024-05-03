package authservice

import (
	"encoding/hex"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/outcaste-io/badger/v3"
	"github.com/outcaste-io/badger/v3/options"
	"storj.io/edge/pkg/auth/authdb"
	bpb "storj.io/edge/pkg/auth/badgerauth/pb"
)

type ReadAuth struct {
	Path string
	Key  string `arg:""`
}

func (r ReadAuth) Run() error {
	opt := badger.DefaultOptions(r.Path)

	// We want to fsync after each write to ensure we don't lose data:
	opt = opt.WithSyncWrites(true)
	opt = opt.WithCompactL0OnClose(true)
	// Currently, we don't want to compress because authservice is mostly
	// deployed in environments where filesystem-level compression is on:
	opt = opt.WithCompression(options.None)
	// If compression and encryption are disabled, adding a cache will lead to
	// unnecessary overhead affecting read performance. Let's disable it then:
	opt = opt.WithBlockCacheSize(0)

	db, err := badger.Open(opt)
	if err != nil {
		return err
	}
	defer db.Close()

	var ek authdb.EncryptionKey
	if err := ek.FromBase32(r.Key); err != nil {
		return err
	}

	//err = db.View(func(txn *badger.Txn) error {
	//	opts := badger.DefaultIteratorOptions
	//	opts.PrefetchSize = 10
	//	it := txn.NewIterator(opts)
	//	defer it.Close()
	//	for it.Rewind(); it.Valid(); it.Next() {
	//		item := it.Item()
	//		k := item.Key()
	//		err := item.Value(func(v []byte) error {
	//			fmt.Printf("key=%s, value=%s\n", hex.EncodeToString(k))
	//			return nil
	//		})
	//		if err != nil {
	//			return err
	//		}
	//	}
	//	return nil
	//})
	//if err != nil {
	//	return err
	//}

	err = db.View(func(txn *badger.Txn) error {

		item, err := txn.Get(ek.ToBinary())
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var loaded bpb.Record
			err = proto.Unmarshal(val, &loaded)
			if err != nil {
				return err
			}
			fmt.Println(proto.MarshalTextString(&loaded))
			fmt.Println("macaroon_head", hex.EncodeToString(loaded.MacaroonHead))
			return nil
		})
	})
	return err

}
