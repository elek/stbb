package metainfo

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"math/rand"
	"os"
	"storj.io/common/grant"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/cmd/uplink/ulloc"
	"time"
)

type BeginObject struct {
	util.DialerHelper
	Key string `help:"key in the form of sj://bucket/encryptedpath" arg:""`
}

func (m BeginObject) Run() error {
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

	pi, err := client.BeginObject(ctx, &pb.BeginObjectRequest{
		Header: &pb.RequestHeader{
			ApiKey: []byte(access.APIKey.SerializeRaw()),
		},
		Bucket:             []byte(bucket),
		EncryptedObjectKey: []byte(fmt.Sprintf("%s-%d-%d", key, rand.Int())),
		EncryptionParameters: &pb.EncryptionParameters{
			CipherSuite: pb.CipherSuite_ENC_AESGCM,
			BlockSize:   256,
		},
	})
	if err != nil {
		return err
	}
	fmt.Println(proto.MarshalTextString(pi))
	return nil
}
