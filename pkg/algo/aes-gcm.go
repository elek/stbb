package piece

import (
	"crypto/rand"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/common/encryption"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "aes-gcm",
		Short: "Test aes-gcm encryption speed",
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return AesGCM(*samples)
	}
	AlgoCmd.AddCommand(cmd)

}

func AesGCM(s int) error {
	key, err := storj.NewKey([]byte("Welcome1"))
	if err != nil {
		return err
	}
	nonce, err := storj.NonceFromBytes([]byte("123456789012345678901234"))
	if err != nil {
		return err
	}

	encrypter, err := encryption.NewEncrypter(storj.EncAESGCM, key, &nonce, int(256*29*memory.B.Int32()))
	if err != nil {
		return err
	}

	in := make([]byte, 32*1024)
	_, err = rand.Read(in)

	if err != nil {
		return err
	}
	start := time.Now()
	bytes := 0
	for i := 0; i < s; i++ {
		out := make([]byte, 0)
		z, err := encrypter.Transform(out, in, int64(i))
		bytes += len(z)
		if err != nil {
			return err
		}
	}
	duration := time.Since(start)
	fmt.Printf("%d Mbytes are encrypted under %f sec, which is %f Mbytes/sec\n", bytes/1024/1024, duration.Seconds(), float64(bytes)/float64(duration.Milliseconds())*1000/1024/1024)
	return nil

}
