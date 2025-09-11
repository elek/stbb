package jobq

import (
	"context"
	"path/filepath"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/storj"
	"storj.io/storj/satellite/jobq"
)

type Inspect struct {
	Server    string `required:"true"`
	Identity  string `required:"true"`
	Placement int    `required:"true"`
	StreamID  string `arg:""`
}

func (s *Inspect) Run() error {
	ctx := context.Background()

	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	cfg := identity.Config{
		CertPath: filepath.Join(s.Identity, "identity.cert"),
		KeyPath:  filepath.Join(s.Identity, "identity.key"),
	}
	identity, err := cfg.Load()
	if err != nil {
		return errors.WithStack(err)
	}
	opts := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}
	tlsOpts, err := tlsopts.NewOptions(identity, opts, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	serverNodeURL, err := storj.ParseNodeURL(s.Server)
	if err != nil {
		return errors.WithStack(err)
	}

	dialer := jobq.NewDialer(tlsOpts)

	conn, err := dialer.DialNodeURL(ctx, serverNodeURL)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	client := jobq.WrapConn(conn)
	defer client.Close()

	inspect, err := client.Inspect(ctx, storj.PlacementConstraint(s.Placement), su, sp.Encode())
	if err != nil {
		return errors.WithStack(err)
	}
	util.PrintStruct(inspect)

	return nil
}
