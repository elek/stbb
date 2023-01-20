package perf

import (
	"context"
	"github.com/elek/stbb/pkg/store/boltstore"
	"github.com/elek/stbb/pkg/store/sqlite"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"os"
	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/storage"

	"storj.io/storj/storage/filestore"
	"storj.io/storj/storagenode/pieces"
	"testing"
	"time"
)

type factory func(b *testing.B, name string, ctx *testcontext.Context, sync string) storage.Blobs

func BenchmarkDirNoSync(b *testing.B) {
	ctx := testcontext.NewWithContextAndTimeout(context.Background(), b, 1*time.Hour)
	defer ctx.Cleanup()

	blobs := createFileBlob(b, "dir-sync", ctx, false)
	bulkWriteTest(ctx, b, blobs, pb.PieceHashAlgorithm_SHA256)
	defer ctx.Check(blobs.Close)
}

func BenchmarkBolt(b *testing.B) {
	ctx := testcontext.NewWithContextAndTimeout(context.Background(), b, 1*time.Hour)
	defer ctx.Cleanup()

	blobs := createBoltBlob(b, "bolt", ctx, false)
	bulkWriteTest(ctx, b, blobs, pb.PieceHashAlgorithm_SHA256)
	defer ctx.Check(blobs.Close)
}

func bulkWriteTest(ctx context.Context, b *testing.B, blobs storage.Blobs, hashAlgo pb.PieceHashAlgorithm) {
	// setup test parameters
	const blockSize = int(256 * memory.KiB)
	satelliteID := testrand.NodeID()
	source := testrand.Bytes(2319872)

	store := pieces.NewStore(zap.NewNop(), blobs, nil, nil, nil, pieces.DefaultConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pieceID := testrand.PieceID()
		writer, err := store.Writer(ctx, satelliteID, pieceID, hashAlgo)
		require.NoError(b, err)

		data := source
		//for len(data) > 0 {
		//	n := blockSize
		//	if n > len(data) {
		//		n = len(data)
		//	}
		//	_, err = writer.Write(data[:n])
		//	require.NoError(b, err)
		//	data = data[n:]
		//}

		_, err = writer.Write(data)
		require.NoError(b, err)

		b.SetBytes(int64(len(data)))
		require.NoError(b, writer.Commit(ctx, &pb.PieceHeader{}))
	}

}

func createBoltBlob(b *testing.B, name string, ctx *testcontext.Context, skipSync bool) storage.Blobs {
	workingDir := ctx.Dir(name)
	_ = os.MkdirAll(workingDir, 0755)
	blobs, err := boltstore.NewBlobStore()
	require.NoError(b, err)
	return blobs
}

func createSqliteBlob(b *testing.B, name string, ctx *testcontext.Context, skipSync bool) storage.Blobs {
	blobs, err := sqlite.NewBlobStore(name+".db", !skipSync)
	require.NoError(b, err)
	return blobs
}

func createFileBlob(b *testing.B, name string, ctx *testcontext.Context, skipSync bool) storage.Blobs {
	workingDir := ctx.Dir("pieces")
	_ = os.MkdirAll(workingDir, 0755)
	dir, err := filestore.NewDir(zap.NewNop(), workingDir)
	require.NoError(b, err)
	//dir.SkipSync = skipSync
	blobs := filestore.New(zap.NewNop(), dir, filestore.DefaultConfig)
	return blobs
}
