package piece

import (
	"bytes"
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"os"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
	"strconv"
	"strings"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "node-speed <nodefile> <satelliteID> <sj://bucket/encrypted-path>",
		Short: "Measure raw node speed performance with downloading one piece",
	}

	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return nodeSpeed(args[0], args[1], args[2], *samples)
	}
	PieceCmd.AddCommand(cmd)

}

func nodeSpeed(nodeFile string, satelliteID string, path string, samples int) error {
	ctx := context.Background()

	quicDialer, err := getDialer(ctx, true)
	if err != nil {
		return err
	}

	tcpDialer, err := getDialer(ctx, false)
	if err != nil {
		return err
	}

	nodes, err := os.ReadFile(nodeFile)
	if err != nil {
		return err
	}

	limitCreator, err := NewKeySigner()
	if err != nil {
		return err
	}

	satelliteURL, err := storj.ParseNodeURL(satelliteID)
	if err != nil {
		return err
	}

	for _, k := range strings.Split(string(nodes), "\n") {
		parts := strings.Split(k, " ")
		storagenodeURL, err := storj.ParseNodeURL(parts[0])
		if err != nil {
			return err
		}

		pieceID, err := storj.PieceIDFromString(parts[1])
		if err != nil {
			return err
		}

		size, err := strconv.Atoi(parts[2])
		if err != nil {
			return err
		}

		timeout := 15 * time.Second
		tcpConnectTime := measured(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			conn, err := tcpDialer.DialNodeURL(ctx, storagenodeURL)
			if err != nil {
				return err
			}
			return conn.Close()
		})

		orderLimit, priv, _, err := limitCreator.CreateOrderLimit(ctx, pieceID, int64(size), satelliteURL.ID, storagenodeURL.ID)
		if err != nil {
			return err
		}

		tcpDownloadTime := measured(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			_, err := downloadPiece(ctx, tcpDialer, orderLimit, storagenodeURL, priv)
			return err
		})

		orderLimit, priv, _, err = limitCreator.CreateOrderLimit(ctx, pieceID, int64(size), satelliteURL.ID, storagenodeURL.ID)
		if err != nil {
			return err
		}

		quicConnectTime := measured(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			conn, err := quicDialer.DialNodeURL(ctx, storagenodeURL)
			if err != nil {
				return err
			}
			return conn.Close()
		})

		quicDownloadTime := measured(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			_, err := downloadPiece(ctx, quicDialer, orderLimit, storagenodeURL, priv)
			return err
		})

		fmt.Printf("%s %d %d %d %d\n", storagenodeURL, tcpConnectTime, tcpDownloadTime, quicConnectTime, quicDownloadTime)

	}

	return nil
}

func measured(exec func() error) int64 {
	start := time.Now()
	err := exec()
	if err != nil {
		return 0
	}
	return time.Since(start).Milliseconds()
}

func downloadPiece(ctx context.Context, dialer rpc.Dialer, limit *pb.OrderLimit, node storj.NodeURL, pk storj.PiecePrivateKey) (int64, error) {
	config := piecestore.DefaultConfig
	client, err := piecestore.Dial(ctx, dialer, node, config)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	download, err := client.Download(ctx, limit, pk, 0, limit.Limit)
	if err != nil {
		return 0, err
	}
	defer download.Close()

	buf := bytes.Buffer{}
	downloaded, err := io.Copy(&buf, download)
	if err != nil {
		return 0, err
	}
	return downloaded, nil
}
