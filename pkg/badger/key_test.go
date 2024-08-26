package badger

import (
	"github.com/stretchr/testify/require"
	"storj.io/storj/storagenode/blobstore"
	"testing"
	"time"
)

func TestKey(t *testing.T) {
	n := time.Now().Truncate(time.Second)
	s := 1234
	k := key(blobstore.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key"),
	}, n, s)

	n2, s2 := stat(k)
	require.Equal(t, n, n2)
	require.Equal(t, s, s2)
}
