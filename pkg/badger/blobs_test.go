package badger

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
	"io"
	"storj.io/common/testcontext"
	"storj.io/storj/storagenode/blobstore"
	"testing"
	"time"
)

func TestReadWrite(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	store, err := NewBlobStore(ctx.Dir(t.TempDir()))
	require.NoError(t, err)

	ref1 := blobstore.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	}

	out, err := store.Create(ctx, ref1)
	require.NoError(t, err)
	_, err = out.Write([]byte("1234567890"))
	require.NoError(t, err)
	require.NoError(t, out.Commit(ctx))

	a, err := store.Open(ctx, ref1)
	require.NoError(t, err)

	all, err := io.ReadAll(a)
	require.NoError(t, err)

	require.Equal(t, []byte("1234567890"), all)
}

func ref(ns string, key string) blobstore.BlobRef {
	return blobstore.BlobRef{
		Namespace: []byte(ns),
		Key:       []byte(key),
	}
}

func save(ctx context.Context, store blobstore.Blobs, ref blobstore.BlobRef, raw string) error {
	out, err := store.Create(ctx, ref)
	if err != nil {
		return errs.Wrap(err)
	}
	_, err = out.Write([]byte(raw))
	if err != nil {
		return errs.Wrap(err)
	}
	err = out.Commit(ctx)
	if err != nil {
		return errs.Wrap(err)
	}
	return nil
}

func TestWalkNamespace(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	store, err := NewBlobStore(ctx.Dir(t.TempDir()))
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		bytes := make([]byte, i)
		err = save(ctx, store, ref("ns1", fmt.Sprintf("key%d", i)), string(bytes))
		require.NoError(t, err)
	}

	err = store.WalkNamespace(ctx, []byte("ns1"), "", func(info blobstore.BlobInfo) error {
		fileInfo, err := info.Stat(ctx)
		if err != nil {
			return errs.Wrap(err)
		}
		require.True(t, time.Since(fileInfo.ModTime()) < 1*time.Minute)
		require.Equal(t, fmt.Sprintf("key%d", fileInfo.Size()), string(info.BlobRef().Key))
		return nil
	})
	require.NoError(t, err)
}

func TestDelete(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	store, err := NewBlobStore(ctx.Dir(t.TempDir()))
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		ref1 := blobstore.BlobRef{
			Namespace: []byte("ns"),
			Key:       []byte(fmt.Sprintf("key%d", i)),
		}

		out, err := store.Create(ctx, ref1)
		require.NoError(t, err)
		_, err = out.Write([]byte("123456789012345678901234567890123456789012345678901234567890"))
		require.NoError(t, err)
		require.NoError(t, out.Commit(ctx))
	}

	for i := 0; i < 100; i++ {
		ref1 := blobstore.BlobRef{
			Namespace: []byte("ns"),
			Key:       []byte(fmt.Sprintf("key%d", i)),
		}

		err = store.Delete(ctx, ref1)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		ref1 := blobstore.BlobRef{
			Namespace: []byte("ns"),
			Key:       []byte(fmt.Sprintf("key%d", i)),
		}

		_, err = store.Open(ctx, ref1)
		require.Error(t, err)
	}

}

func TestMoveToTrash(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	store, err := NewBlobStore(ctx.Dir(t.TempDir()))
	require.NoError(t, err)

	ref1 := blobstore.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	}

	out, err := store.Create(ctx, ref1)
	require.NoError(t, err)
	_, err = out.Write([]byte("1234567890"))
	require.NoError(t, err)
	require.NoError(t, out.Commit(ctx))

	require.NoError(t, store.Trash(ctx, ref1, time.Now()))

	_, err = store.Open(ctx, ref1)
	require.Error(t, err)

	trash, err := store.RestoreTrash(ctx, []byte("ns"))
	require.NoError(t, err)

	require.Equal(t, len(trash), 1)

	a, err := store.Open(ctx, ref1)
	require.NoError(t, err)

	all, err := io.ReadAll(a)
	require.NoError(t, err)

	require.Equal(t, []byte("1234567890"), all)
}

func TestWriteWithSeek(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()
	//
	store, err := NewBlobStore(ctx.Dir(t.TempDir()))
	require.NoError(t, err)
	//store, err := filestore.NewAt(zap.NewNop(), "/tmp/q", filestore.Config{
	//	WriteBufferSize: 1024,
	//})
	//require.NoError(t, err)

	ref1 := blobstore.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	}

	out, err := store.Create(ctx, ref1)
	require.NoError(t, err)

	_, err = out.Seek(10, io.SeekStart)
	require.NoError(t, err)

	_, err = out.Write([]byte("1234567890"))
	require.NoError(t, err)

	_, err = out.Write([]byte("abcdefghijkl"))
	require.NoError(t, err)

	_, err = out.Seek(0, io.SeekStart)
	require.NoError(t, err)

	_, err = out.Write([]byte("ST"))
	require.NoError(t, err)

	_, err = out.Write([]byte("MNB"))
	require.NoError(t, err)

	_, err = out.Seek(31, io.SeekStart)
	require.NoError(t, err)

	require.NoError(t, out.Commit(ctx))

	a, err := store.Open(ctx, ref1)
	require.NoError(t, err)
	defer a.Close()

	all, err := io.ReadAll(a)
	require.NoError(t, err)

	require.Equal(t, []byte("STMNB\x00\x00\x00\x00\x001234567890abcdefghijk"), all)

}

func TestReopen(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	d := ctx.Dir("storage")

	store, err := NewBlobStore(d)
	require.NoError(t, err)

	create, err := store.Create(ctx, blobstore.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	})
	require.NoError(t, err)

	_, err = create.Write([]byte("test"))
	require.NoError(t, err)

	err = create.Commit(ctx)
	require.NoError(t, err)

	require.NoError(t, store.Close())

	store, err = NewBlobStore(d)
	require.NoError(t, err)

	ns, err := store.ListNamespaces(ctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(ns))
}

func TestMultiWrite(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	d := ctx.Dir("storage")

	store, err := NewBlobStore(d)
	require.NoError(t, err)

	ref := blobstore.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	}
	create, err := store.Create(ctx, ref)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = create.Write([]byte("test"))
		require.NoError(t, err)
	}

	_, err = create.Write([]byte(""))
	require.NoError(t, err)

	err = create.Commit(ctx)
	require.NoError(t, err)

	reader, err := store.Open(ctx, ref)
	defer reader.Close()
	content, err := rall(reader)
	require.NoError(t, err)
	require.Equal(t, 10*4, len(content))
	require.Equal(t, "testtesttesttesttesttesttesttesttesttest", string(content))
}

func rall(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 1)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}
