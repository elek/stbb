package jobq

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/storj"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/jobq"
)

type Submit struct {
	Server                   string  `required:"true"`
	Identity                 string  `required:"true"`
	StreamID                 string  `required:"true" help:"Stream ID (UUID format)"`
	Position                 uint64  `default:"0" help:"Segment position"`
	Placement                uint16  `default:"0" help:"Placement constraint"`
	Health                   float64 `default:"0.5" help:"Segment health (0.0-1.0)"`
	NumNormalizedHealthy     int16   `default:"0" help:"Number of normalized healthy pieces"`
	NumNormalizedRetrievable int16   `default:"0" help:"Number of normalized retrievable pieces"`
	NumOutOfPlacement        int16   `default:"0" help:"Number of out-of-placement pieces"`
}

func (s *Submit) Run() error {
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

	streamID, err := uuid.FromString(s.StreamID)
	if err != nil {
		return errors.Wrap(err, "invalid stream ID")
	}

	job := jobq.RepairJob{
		ID: jobq.SegmentIdentifier{
			StreamID: streamID,
			Position: s.Position,
		},
		Health:                   s.Health,
		Placement:                s.Placement,
		NumNormalizedHealthy:     s.NumNormalizedHealthy,
		NumNormalizedRetrievable: s.NumNormalizedRetrievable,
		NumOutOfPlacement:        s.NumOutOfPlacement,
	}

	wasNew, err := client.Push(ctx, job)
	if err != nil {
		return errors.WithStack(err)
	}

	if wasNew {
		fmt.Printf("Successfully submitted new job: %s/%d\n", streamID, s.Position)
	} else {
		fmt.Printf("Job already exists: %s/%d (updated)\n", streamID, s.Position)
	}

	return nil
}
