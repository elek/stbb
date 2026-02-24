package admin

import (
	"context"
	"fmt"

	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/storj"
	"storj.io/storj/satellite/overlay"
)

type Unsuspend struct {
	db.WithDatabase
	NodeID storj.NodeID `arg:"" required:"" help:"node ID to unsuspend"`
}

func (u *Unsuspend) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := u.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer satelliteDB.Close()

	node, err := satelliteDB.OverlayCache().Get(ctx, u.NodeID)
	if err != nil {
		return errors.Wrap(err, "failed to get node from overlay")
	}

	fmt.Println("Node:", u.NodeID)
	fmt.Println("  unknown_audit_suspended:", node.UnknownAuditSuspended)
	fmt.Println("  offline_suspended:", node.OfflineSuspended)
	fmt.Println("  disqualified:", node.Disqualified)

	if node.UnknownAuditSuspended == nil && node.OfflineSuspended == nil {
		fmt.Println("Node is not suspended.")
		return nil
	}

	// Clear suspension flags in the nodes table.
	err = satelliteDB.OverlayCache().UpdateReputation(ctx, u.NodeID, overlay.ReputationUpdate{})
	if err != nil {
		return errors.Wrap(err, "failed to clear suspension in nodes table")
	}
	fmt.Println("Cleared suspension flags in nodes table.")

	// Clear unknown_audit_suspended in reputation table.
	err = satelliteDB.Reputation().UnsuspendNodeUnknownAudit(ctx, u.NodeID)
	if err != nil {
		return errors.Wrap(err, "failed to clear unknown_audit_suspended in reputation table")
	}
	fmt.Println("Cleared unknown_audit_suspended in reputation table.")

	// Reset offline_suspended, under_review, and reputation scores in reputation table.
	_, err = satelliteDB.Testing().RawDB().ExecContext(ctx, `
		UPDATE reputations SET
			offline_suspended = NULL,
			under_review = NULL,
			unknown_audit_reputation_alpha = 1,
			unknown_audit_reputation_beta = 0,
			online_score = 1
		WHERE id = $1
	`, u.NodeID.Bytes())
	if err != nil {
		return errors.Wrap(err, "failed to reset reputation scores")
	}
	fmt.Println("Reset offline_suspended, under_review, and reputation scores in reputation table.")

	// Verify the result.
	repInfo, err := satelliteDB.Reputation().Get(ctx, u.NodeID)
	if err != nil {
		return errors.Wrap(err, "failed to get reputation info")
	}
	fmt.Println("\nAfter unsuspend:")
	fmt.Println("  unknown_audit_suspended:", repInfo.UnknownAuditSuspended)
	fmt.Println("  offline_suspended:", repInfo.OfflineSuspended)
	fmt.Println("  under_review:", repInfo.UnderReview)
	fmt.Println("  unknown_audit_reputation_alpha:", repInfo.UnknownAuditReputationAlpha)
	fmt.Println("  unknown_audit_reputation_beta:", repInfo.UnknownAuditReputationBeta)
	fmt.Println("  online_score:", repInfo.OnlineScore)

	return nil
}
