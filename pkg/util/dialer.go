package util

import (
	"context"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/quic"
)

func GetDialer(ctx context.Context, pooled bool, forceQuic bool) (rpc.Dialer, error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return rpc.Dialer{}, err
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return rpc.Dialer{}, err
	}
	var dialer rpc.Dialer
	if pooled {
		dialer = rpc.NewDefaultPooledDialer(tlsOptions)
	} else {
		dialer = rpc.NewDefaultDialer(tlsOptions)
	}
	if forceQuic {
		dialer.Connector = quic.NewDefaultConnector(nil)
	} else {
		dialer.Connector = rpc.NewDefaultTCPConnector(nil)
	}
	return dialer, nil
}
