package bloom

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/internalpb"
	"storj.io/uplink/private/piecestore"
)

type SendClient struct {
	util.DialerHelper
	URL    storj.NodeURL
	Filter string `arg:""`
}

func (s SendClient) Run() error {
	ctx := context.Background()
	dialer, err := s.DialerHelper.CreateRPCDialer()
	if err != nil {
		return errors.WithStack(err)
	}
	client, err := piecestore.Dial(ctx, dialer, s.URL, piecestore.DefaultConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	defer client.Close()

	filter, err := os.ReadFile(s.Filter)
	if err != nil {
		return errors.WithStack(err)
	}

	retainInfo := &internalpb.RetainInfo{}
	err = pb.Unmarshal(filter, retainInfo)
	if retainInfo.StorageNodeId != s.URL.ID {
		panic("invalid storagenode " + retainInfo.StorageNodeId.String())
	}

	err = client.Retain(ctx, &pb.RetainRequest{
		CreationDate: retainInfo.CreationDate,
		Filter:       retainInfo.Filter,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
