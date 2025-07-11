package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"strings"
)

type Audit struct {
	db.WithDatabase
	util.DialerHelper
	NodeURL   storj.NodeURL `reuired:"" help:"Node address"`
	Keys      string        `required:"" help:"the satellite identity directory"`
	PieceFile string        `required:"" help:"CSV file with piece information"`
}

func (a *Audit) Run() error {
	ctx := context.Background()

	// Load satellite identity
	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(a.Keys, "identity.cert"),
		KeyPath:  filepath.Join(a.Keys, "identity.key"),
	}
	ident, err := satelliteIdentityCfg.Load()
	if err != nil {
		return err
	}

	// Set up key signer and dialer
	keySigner := util.NewKeySignerFromFullIdentity(ident, pb.PieceAction_GET_REPAIR)
	dialer, err := util.GetDialerForIdentity(ctx, ident, true, false)
	if err != nil {
		return err
	}

	// Read piece file
	raw, err := os.ReadFile(a.PieceFile)
	if err != nil {
		return errors.WithStack(err)
	}

	// Connect to storage node
	conn, err := dialer.DialNode(ctx, a.NodeURL, rpc.DialOptions{})
	if err != nil {
		return errors.Wrap(err, "couldn't dial node")
	}
	defer conn.Close()

	// Create client
	client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))

	// Process each line in the CSV file
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) < 4 {
			continue
		}

		pieceID, err := storj.PieceIDFromString(parts[4])
		if err != nil {
			fmt.Printf("Invalid piece ID %s: %v\n", parts[0], err)
			continue
		}

		// Audit the piece
		err = a.auditPiece(ctx, client, keySigner, ident.ID, pieceID)
		if err != nil {
			fmt.Printf("Audit failed for piece %s on node %s: %v\n", pieceID, a.NodeURL.ID, err)
		}
	}

	return nil
}

func (a *Audit) auditPiece(ctx context.Context, client pb.DRPCPiecestoreClient, keySigner *util.KeySigner, satelliteID storj.NodeID, pieceID storj.PieceID) error {
	_, _, err := util.DownloadPiece(ctx, client, keySigner, util.DownloadRequest{
		PieceID:     pieceID,
		Storagenode: a.NodeURL,
		Size:        1,
		SatelliteID: satelliteID,
	}, func(bytes []byte, hash *pb.PieceHash, ol *pb.OrderLimit) {
		if hash == nil {
			fmt.Printf("AUDIT_RESULT: piece_empty,%s,%s\n", pieceID, a.NodeURL.ID)
			return
		}
		if len(hash.Hash) == 0 {
			fmt.Printf("AUDIT_RESULT: zero_piece_hash,%s,%s\n", pieceID, a.NodeURL.ID)
			return
		}
		fmt.Printf("AUDIT_RESULT: success,%s,%s\n", pieceID, a.NodeURL.ID)
	})

	if err != nil {
		return errors.Wrap(err, "download piece failed")
	}

	return nil
}
