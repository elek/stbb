package util

import (
	"context"
	flag "github.com/spf13/pflag"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/quic"
	"storj.io/common/storj"
)

type DialerHelper struct {
	Quic        bool
	Pooled      bool
	Noise       bool
	IdentityDir string
}

type Dialer func(ctx context.Context, nodeURL storj.NodeURL) (_ *rpc.Conn, err error)

func NewDialerHelper(flagSet *flag.FlagSet) *DialerHelper {
	d := DialerHelper{}
	if flagSet != nil {
		flagSet.BoolVarP(&d.Quic, "quic", "q", false, "Force to use quic protocol")
		flagSet.BoolVar(&d.Noise, "noise", false, "Force to use NOISE protocol")
	}
	return &d
}

func (d *DialerHelper) Connect(ctx context.Context, nodeURL storj.NodeURL) (*rpc.Conn, error) {
	cd, err := d.CreateDialer()
	if err != nil {
		return nil, err
	}
	return cd(ctx, nodeURL)
}

func (d *DialerHelper) CreateRPCDialer() (rpc.Dialer, error) {
	ident, err := identity.NewFullIdentity(context.Background(), identity.NewCAOptions{
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
	if d.Pooled {
		dialer = rpc.NewDefaultPooledDialer(tlsOptions)
	} else {
		dialer = rpc.NewDefaultDialer(tlsOptions)
	}
	if d.Quic {
		dialer.Connector = quic.NewDefaultConnector(nil)
	} else {
		dialer.Connector = rpc.NewDefaultTCPConnector(nil)
	}
	return dialer, nil
}

func (d *DialerHelper) CreateDialer() (Dialer, error) {
	dialer, err := d.CreateRPCDialer()
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, nodeURL storj.NodeURL) (_ *rpc.Conn, err error) {
		return dialer.DialNode(ctx, nodeURL, rpc.DialOptions{
			ReplaySafe: d.Noise,
		})
	}, nil
}

func GetDialer(ctx context.Context, pooled bool, forceQuic bool) (rpc.Dialer, error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return rpc.Dialer{}, err
	}
	return GetDialerForIdentity(ctx, ident, pooled, forceQuic)
}

func GetDialerForIdentity(ctx context.Context, ident *identity.FullIdentity, pooled bool, forceQuic bool) (rpc.Dialer, error) {

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
