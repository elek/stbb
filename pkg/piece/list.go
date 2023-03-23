package piece

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/grant"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink/private/metaclient"
)

type List struct {
	util.DialerHelper
	Path string `arg:"" help:"Key url (sj://bucket/.../key)"`
}

func (l *List) Run() error {
	p, err := ulloc.Parse(l.Path)
	if err != nil {
		return err
	}
	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", l.Path)
	}

	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	dialer, err := l.CreateRPCDialer()
	if err != nil {
		return err
	}

	access, err := grant.ParseAccess(gr)
	if err != nil {
		return err
	}
	metainfoClient, err := metaclient.DialNodeURL(ctx,
		dialer,
		access.SatelliteAddress,
		access.APIKey,
		"stbb")
	if err != nil {
		return err
	}
	defer metainfoClient.Close()

	decoded, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return err
	}

	resp, err := metainfoClient.GetObjectIPs(ctx, metaclient.GetObjectIPsParams{
		Bucket:             []byte(bucket),
		EncryptedObjectKey: decoded,
		Version:            0,
	})
	if err != nil {
		return err
	}
	for _, k := range resp.IPPorts {
		if k != nil {
			fmt.Println(string(k))
		}
	}

	return nil
}
