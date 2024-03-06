package metainfo

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/grant"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/cmd/uplink/ulloc"
	"time"
)

type DownloadObject struct {
	util.DialerHelper
	Key string `help:"key in the form of sj://bucket/encryptedpath"`
}

func (m DownloadObject) Run() error {
	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
	defer cancel()
	dialer, err := m.CreateDialer()
	if err != nil {
		return err
	}

	access, err := grant.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	satelliteURL, err := storj.ParseNodeURL(access.SatelliteAddress)
	if err != nil {
		return err
	}

	conn, err := dialer(ctx, satelliteURL)
	client := pb.NewDRPCMetainfoClient(conn)

	p, err := ulloc.Parse(m.Key)
	if err != nil {
		return err
	}
	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errors.WithStack(err)
	}

	pi, err := client.DownloadObject(ctx, &pb.DownloadObjectRequest{
		Header: &pb.RequestHeader{
			ApiKey: []byte(access.APIKey.Serialize()),
		},
		Bucket:             []byte(bucket),
		EncryptedObjectKey: []byte(key),
	})
	if err != nil {
		return err
	}
	fmt.Println(proto.MarshalTextString(pi))
	return nil
}

//func download(s string, samples int, pooled bool, quic bool, verbose bool) error {
//	ctx := context.Background()
//	gr := os.Getenv("UPLINK_ACCESS")
//
//	p, err := ulloc.Parse(s)
//	if err != nil {
//		return err
//	}
//	bucket, key, ok := p.RemoteParts()
//	if !ok {
//		return errs.New("Path is not remote %s", s)
//	}
//
//	dialer, err := util.GetDialer(ctx, pooled, quic)
//	if err != nil {
//		return err
//	}
//
//	access, err := grant.ParseAccess(gr)
//	if err != nil {
//		return err
//	}
//
//	decoded, err := base64.URLEncoding.DecodeString(key)
//	if err != nil {
//		return err
//	}
//
//	durationMs := int64(0)
//	for i := 0; i < samples; i++ {
//		start := time.Now()
//		err = DoOnce(ctx, dialer, access.SatelliteAddress, access.APIKey, bucket, decoded)
//		elapsed := time.Since(start).Milliseconds()
//		durationMs += elapsed
//		if err != nil {
//			return err
//		}
//
//		if verbose {
//			fmt.Println(elapsed)
//		}
//	}
//	if pooled {
//		samples = samples - 1
//	}
//	fmt.Printf("Executed %d test during %d ms (%f ms / req)\n", samples, durationMs, float64(durationMs)/float64(samples))
//
//	return nil
//}
//
//func DoOnce(ctx context.Context, dialer rpc.Dialer, nodeURL string, apiKey *macaroon.APIKey, bucket string, encryptedObjectKey []byte) (err error) {
//	defer mon.Task()(&ctx)(&err)
//	metainfoClient, err := metaclient.DialNodeURL(ctx,
//		dialer,
//		nodeURL,
//		apiKey,
//		"stbb")
//	if err != nil {
//		return err
//	}
//	defer metainfoClient.Close()
//
//	_, err = metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
//		Bucket:             []byte(bucket),
//		EncryptedObjectKey: encryptedObjectKey,
//	})
//
//	return err
//}
