package piece

import (
	"crypto/sha256"
	"crypto/sha512"
	"flag"
	"fmt"
	"github.com/klauspost/cpuid/v2"
	sha256simd "github.com/minio/sha256-simd"
	"github.com/spf13/cobra"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"
	"golang.org/x/benchmarks/driver"
	"hash"
	"math/rand"
	"strings"
	"time"
)

func init() {
	cmd := &cobra.Command{
		FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
		Use:                "hash",
		Short:              "Test piece hash calculation speed",
	}
	size := cmd.Flags().IntP("size", "n", 657743, "Size of data to be hashed")
	writes := cmd.Flags().IntP("buffer", "b", 1024, "Size of the buffer, used to call hash.Write")
	algo := cmd.Flags().StringP("hash", "a", "sha-256", "Hash algorithm to be used")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		flag.Parse()
		pieceHash(*writes, *size, *algo)
		return nil
	}
	AlgoCmd.AddCommand(cmd)

}

func pieceHash(writes int, size int, algo string) {
	algos := map[string]func() hash.Hash{
		"sha256": sha256.New,
		"sha512": sha512.New,
		"xxh3": func() hash.Hash {
			return xxh3.New()
		},
		"blake3": func() hash.Hash {
			return blake3.New()
		},
		"sha256simd": func() hash.Hash {
			return sha256simd.New()
		},
	}

	var buf = make([]byte, writes)
	rand.Seed(time.Now().Unix())
	rand.Read(buf)

	fmt.Println("algo:", algo)
	fmt.Println("name:", cpuid.CPU.BrandName)
	fmt.Println("Features:", strings.Join(cpuid.CPU.FeatureSet(), " "))

	steps := size / writes
	driver.Main(fmt.Sprintf("%d-%d", writes*steps, writes), func() driver.Result {
		return driver.Benchmark(func(u uint64) {

			creator, found := algos[algo]
			if !found {
				panic("No idea what is hash algorithm " + algo + " use one of (" + keys(algos) + ")")
			}
			hashAlgo := creator()
			sum := make([]byte, hashAlgo.Size())

			for i := uint64(0); i < u; i++ {
				for s := 0; s < steps; s++ {
					hashAlgo.Write(buf[:writes])
				}
				hashAlgo.Sum(sum[:0])
			}
		})
	})
}

func keys(algos map[string]func() hash.Hash) string {
	names := []string{}
	for k, _ := range algos {
		names = append(names, k)
	}
	return strings.Join(names, ",")
}
