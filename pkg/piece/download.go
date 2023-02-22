package piece

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"os"
	"storj.io/common/grant"
	"storj.io/common/identity"
	"storj.io/common/rpc"
	"storj.io/common/storj"
)

type Downloader struct {
	satelliteURL      storj.NodeURL
	storagenodeURL    storj.NodeURL
	fi                *identity.FullIdentity
	OrderLimitCreator OrderLimitCreator
	dialer            rpc.Dialer
	grant             *grant.Access
}

func NewDownloader(ctx context.Context, storagenodeURL string, quic bool, pooled bool) (d Downloader, err error) {
	gr := os.Getenv("UPLINK_ACCESS")
	if gr != "" {
		d.grant, err = grant.ParseAccess(gr)
		if err != nil {
			return d, err
		}
		d.satelliteURL, err = storj.ParseNodeURL(d.grant.SatelliteAddress)
		if err != nil {
			return
		}
	}

	sat := os.Getenv("STBB_SATELLITE")
	if sat != "" {
		d.satelliteURL, err = storj.ParseNodeURL(sat)
		if err != nil {
			return
		}
	}

	d.storagenodeURL, err = storj.ParseNodeURL(storagenodeURL)
	if err != nil {
		return
	}

	d.dialer, err = util.GetDialer(ctx, quic, pooled)
	if err != nil {
		return
	}

	d.OrderLimitCreator, err = NewKeySigner()
	if err != nil {
		return
	}
	return
}
