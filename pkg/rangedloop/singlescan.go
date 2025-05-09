package rangedloop

import (
	"context"
	"github.com/pkg/errors"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/shared/tagsql"
)

type FullScan struct {
	sql *SQLProvider
}

func NewFullScan(db *metabase.DB, scanType string) *FullScan {
	return &FullScan{
		sql: &SQLProvider{
			conn:     db,
			scanType: scanType,
		},
	}
}
func (f *FullScan) CreateRanges(_ context.Context, nRanges int, batchSize int) ([]rangedloop.SegmentProvider, error) {
	if nRanges != 1 {
		return nil, errors.New("Only one segment is allowed")
	}
	return []rangedloop.SegmentProvider{
		f.sql,
	}, nil

}

var _ rangedloop.RangeSplitter = &FullScan{}

type SQLProvider struct {
	conn     *metabase.DB
	scanType string
}

func (s *SQLProvider) Range() rangedloop.UUIDRange {
	end := uuid.Max()
	return rangedloop.UUIDRange{
		Start: new(uuid.UUID),
		End:   &end,
	}
}

func (s *SQLProvider) Iterate(ctx context.Context, fn func([]rangedloop.Segment) error) error {
	panic("TODO: reimplement this with using Adapter")
	//aliasMap, err := s.conn.LatestNodesAliasMap(ctx)
	//if err != nil {
	//	return err
	//}
	//
	//query := `select
	//stream_id, position,
	//		created_at, expires_at, repaired_at,
	//		root_piece_id,
	//		encrypted_size,
	//		plain_offset, plain_size,
	//		redundancy,
	//		remote_alias_pieces,
	//		placement FROM segments WHERE segments.position is not null AND segments.remote_alias_pieces is not null`
	//
	//var args []interface{}
	//switch s.scanType {
	//case "test":
	//	query += " LIMIT 100"
	//case "placement":
	//	query += " AND segments.placement = $1"
	//	args = append(args, storj.PlacementConstraint(12))
	//default:
	//}
	//fmt.Println("executing", query)
	//rows, err := s.conn.UnderlyingTagSQL().QueryContext(context.Background(), query, args...)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//defer rows.Close()
	//segments := make([]rangedloop.Segment, 0, 1000)
	//for rows.Next() {
	//	p := metabase.LoopSegmentEntry{}
	//	err := scanItem(ctx, aliasMap, rows, &p)
	//	if err != nil {
	//		return errors.WithStack(err)
	//	}
	//	segments = append(segments, rangedloop.Segment(p))
	//	if len(segments) > 1000 {
	//		err := fn(segments)
	//		if err != nil {
	//			return err
	//		}
	//		segments = segments[:0]
	//	}
	//	if err != nil {
	//		return err
	//	}
	//}
	//
	//err = fn(segments)
	//if err != nil {
	//	return err
	//}

	return nil
}

var _ rangedloop.SegmentProvider = &SQLProvider{}

func scanItem(ctx context.Context, aliasMap *metabase.NodeAliasMap, row tagsql.Rows, item *metabase.LoopSegmentEntry) error {
	err := row.Scan(
		&item.StreamID, &item.Position,
		&item.CreatedAt, &item.ExpiresAt, &item.RepairedAt,
		&item.RootPieceID,
		&item.EncryptedSize,
		&item.PlainOffset, &item.PlainSize,
		redundancyScheme{&item.Redundancy},
		&item.AliasPieces,
		&item.Placement,
	)
	for _, piece := range item.AliasPieces {
		sn, _ := aliasMap.Node(piece.Alias)
		item.Pieces = append(item.Pieces, metabase.Piece{
			Number:      piece.Number,
			StorageNode: sn,
		})
	}
	if err != nil {
		return errors.WithMessage(err, "failed to scan segments: %w")
	}

	return nil
}
