package badger

import (
	"encoding/binary"
	"storj.io/storj/storagenode/blobstore"
	"time"
)

var namespacePrefix = []byte("nmspc")
var blobPrefix = []byte("blobs")
var trashPrefix = []byte("trash")

func key(ref blobstore.BlobRef, time time.Time, size int) []byte {
	rawStat := make([]byte, 0, 16)
	rawStat = binary.BigEndian.AppendUint64(rawStat, uint64(time.Unix()))
	rawStat = binary.BigEndian.AppendUint64(rawStat, uint64(size))
	res := blobPrefix
	res = append(res, ref.Namespace...)
	res = append(res, ref.Key...)
	res = append(res, rawStat...)
	return res
}

func stat(from []byte) (time.Time, int) {
	key := from[len(from)-16:]
	seconds := binary.BigEndian.Uint64(key[0:8])
	bytes := binary.BigEndian.Uint64(key[8:])
	return time.Unix(int64(seconds), 0), int(bytes)

}

func keyPrefix(ref blobstore.BlobRef) []byte {
	return append(append(blobPrefix, ref.Namespace...), ref.Key...)
}

func trashKey(ref blobstore.BlobRef) []byte {
	return append(append(trashPrefix, ref.Namespace...), ref.Key...)
}
