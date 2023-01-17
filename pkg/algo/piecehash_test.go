package piece

import (
	"github.com/minio/sha256-simd"
	"github.com/zeebo/blake3"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkHashSHA256(b *testing.B) {
	hashAlgo := sha256.New()
	sum := make([]byte, hashAlgo.Size())

	writes := 10024
	var buf = make([]byte, writes)
	rand.Seed(time.Now().Unix())
	rand.Read(buf)

	steps := 657743 / writes
	for i := 0; i < b.N; i++ {
		for s := 0; s < steps; s++ {
			hashAlgo.Write(buf[:writes])
		}
		hashAlgo.Sum(sum[:0])
	}
}

func BenchmarkHashBlake3(b *testing.B) {
	hashAlgo := blake3.New()
	sum := make([]byte, hashAlgo.Size())

	writes := 1024
	var buf = make([]byte, writes)
	rand.Seed(time.Now().Unix())
	rand.Read(buf)

	steps := 657743 / writes
	for i := 0; i < b.N; i++ {
		for s := 0; s < steps; s++ {
			hashAlgo.Write(buf[:writes])
		}
		hashAlgo.Sum(sum[:0])
	}
}
