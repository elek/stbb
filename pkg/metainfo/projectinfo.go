package metainfo

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"os"
	"storj.io/common/grant"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

type ProjectInfo struct {
	util.DialerHelper
}

func (m ProjectInfo) Run() error {
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
	fmt.Println(satelliteURL)

	conn, err := dialer(ctx, satelliteURL)
	client := pb.NewDRPCMetainfoClient(conn)

	pi, err := client.ProjectInfo(ctx, &pb.ProjectInfoRequest{
		Header: &pb.RequestHeader{
			ApiKey: []byte(access.APIKey.Serialize()),
		},
	})
	if err != nil {
		return err
	}
	fmt.Println(pi.GetProjectPublicId())
	fmt.Println(pi.GetProjectSalt())
	return nil
}
