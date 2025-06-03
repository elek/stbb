package jobq

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/storj"
	"storj.io/storj/satellite/jobq"
)

type Peek struct {
	Server        string `required:"true"`
	Identity      string `required:"true"`
	WithHistogram bool
	Placement     *int
}

func (s *Peek) Run() error {
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

	peek, err := client.Peek(ctx, 10, nil, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, job := range peek {
		fmt.Printf("Job ID: %s/%d, Missing: %d, OOP: %d, Helath: %f\n", job.ID.StreamID, job.ID.Position, job.NumMissing, job.NumOutOfPlacement, job.Health)
	}
	return nil
}
