package segment

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/satellitedb/dbx"
	"storj.io/storj/shared/dbutil"
	"storj.io/storj/shared/dbutil/pgutil"
	"strconv"
	"strings"
	"time"
)

// Availability reports the status / availability of one single segment.
type Availability struct {
	StreamID string `arg:""`
	util.DialerHelper
	Keys string `required:""`
}

func (s *Availability) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	ctx := context.TODO()

	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	driver, source, _, err := dbutil.SplitConnStr(os.Getenv("STBB_DB_SATELLITE"))
	if err != nil {
		return err
	}
	source, err = pgutil.EnsureApplicationName(source, "stbb")
	if err != nil {
		return err
	}

	satelliteDBX, err := dbx.Open(driver, source, nil)
	if err != nil {
		return err
	}

	sp := metabase.SegmentPosition{}
	parts := strings.Split(s.StreamID, "/")
	su, err := util.ParseUUID(parts[0])
	if err != nil {
		return err
	}
	if len(parts) > 1 {
		part, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}
		sp = metabase.SegmentPositionFromEncoded(uint64(part))
	}

	segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
		StreamID: su,
		Position: sp,
	})
	if err != nil {
		return err
	}
	fmt.Println("placement", segment.Placement)
	//fmt.Println("size", segment.EncryptedSize/int32(segment.Redundancy.RequiredShares))

	out := csv.NewWriter(os.Stdout)
	out.Write([]string{
		"piece_no",
		"piece_id",
		"node_url",
		"snhex",
		"country_code",
		"email",
		"wallet",
		"last_contact_success",
		"contained",
		"unknown_audit_suspended",
		"offline_suspended",
		"disqualified",
		"exit_initiated",
		"exit_success",
		"version",
		"test",
	})

	defer out.Flush()
	for _, piece := range segment.Pieces {
		node, err := satelliteDBX.Get_Node_By_Id(ctx, dbx.Node_Id(piece.StorageNode.Bytes()))
		if err != nil {
			return err
		}

		pieceID := segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number))

		snURL, err := storj.ParseNodeURL(fmt.Sprintf("%s@%s", piece.StorageNode.String(), safeStr(node.LastIpPort)))
		if err != nil {
			return err
		}
		test := ""
		if s.Keys != "" {
			test = s.testDownload(ctx, snURL, pieceID)
		}

		_ = out.Write([]string{
			fmt.Sprintf("%d", piece.Number),
			fmt.Sprintf("%s", pieceID.String()),
			snURL.String(),
			hex.EncodeToString(piece.StorageNode.Bytes()),
			safeStr(node.CountryCode),
			node.Email,
			node.Wallet,
			node.LastContactSuccess.Format(time.RFC3339Nano),
			safeTime(node.Contained),
			safeTime(node.UnknownAuditSuspended),
			safeTime(node.OfflineSuspended),
			safeTime(node.Disqualified),
			safeTime(node.ExitInitiatedAt),
			fmt.Sprintf("%v", node.ExitSuccess),
			fmt.Sprintf("%d.%d.%d", node.Major, node.Minor, node.Patch),
			test,
		})
		out.Flush()
	}
	return nil
}

func (s *Availability) testDownload(ctx context.Context, url storj.NodeURL, piece storj.PieceID) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(s.Keys, "identity.cert"),
		KeyPath:  filepath.Join(s.Keys, "identity.key"),
	}
	ident, err := satelliteIdentityCfg.Load()
	if err != nil {
		panic(err)
	}
	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)
	if err != nil {
		return "couldn't create dialer: " + err.Error()
	}
	conn, err := dialer.DialNodeURL(ctx, url)
	if err != nil {
		return "couldn't create dialer: " + err.Error()
	}
	defer conn.Close()
	client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))

	response, err := client.Exists(ctx, &pb.ExistsRequest{
		PieceIds: []pb.PieceID{
			piece,
		},
	})
	if err != nil {
		return err.Error()
	}
	if len(response.Missing) > 0 {
		return "missing"
	}
	return "ok"
}

func safeStr(port *string) string {
	if port == nil {
		return ""
	}
	return *port
}

func safeTime(t *time.Time) string {
	if t != nil {
		return t.Format(time.RFC3339)
	}
	return ""
}
