package piece

import (
	"crypto/sha256"
	"github.com/zeebo/blake3"
	"hash"
	lblake3 "lukechampine.com/blake3"
	"math/rand"
	"testing"
	"time"
)

var buf = func() []byte {
	buf := make([]byte, 10*1024*1024)
	rand.Seed(time.Now().Unix())
	rand.Read(buf)
	return buf
}()

func benchMarkHash(c func() hash.Hash, n int) {
	var sum [256]byte
	maxBytes := 1024
	writes := len(buf)
	//maxBytes := 2314098
	steps := maxBytes / writes
	for i := 0; i < n; i++ {
		hashAlgo := c()
		for s := 0; s <= steps; s++ {
			hashAlgo.Write(buf[:writes])
		}
		hashAlgo.Sum(sum[:0])
	}
}

func BenchmarkHashSHA256(b *testing.B) {
	benchMarkHash(func() hash.Hash { return sha256.New() }, b.N)

}

func BenchmarkHashBlake3(b *testing.B) {
	benchMarkHash(func() hash.Hash { return blake3.New() }, b.N)
}

func BenchmarkHashLBlake3(b *testing.B) {
	benchMarkHash(func() hash.Hash { return lblake3.New(32, nil) }, b.N)
}
