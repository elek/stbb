package jobq

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/storj"
	"storj.io/storj/satellite/jobq"
)

type Trim struct {
	Server    string `required:"true"`
	Identity  string `required:"true"`
	Placement *int   `required:"true"`
}

func (s *Trim) Run() error {
	ctx := context.Background()

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

	removedSegments, err := client.Trim(ctx, storj.PlacementConstraint(*s.Placement), 0)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("Trimmed placement", *s.Placement, ", removed segments:", removedSegments)
	return nil
}
