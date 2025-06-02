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

type Stat struct {
	Server   string `required:"true"`
	Identity string `required:"true"`
}

func (s *Stat) Run() error {
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

	stats, err := client.StatAll(ctx, false)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, stat := range stats {
		fmt.Println("Placement", stat.Placement)
		fmt.Println("Count", stat.Count)
		fmt.Println("MaxSegmentHealth", stat.MaxSegmentHealth)
		fmt.Println("MinSegmentHealth", stat.MinSegmentHealth)
		fmt.Println()

	}
	return nil

}
