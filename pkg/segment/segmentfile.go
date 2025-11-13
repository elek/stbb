package segment

import (
	"os"
	"time"

	"storj.io/common/storj"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
)

import (
	"gopkg.in/yaml.v3"
)

type AliasPiece struct {
	Number int `yaml:"number"`
	Alias  int `yaml:"alias"`
}

type Piece struct {
	Number      int          `yaml:"number"`
	StorageNode storj.NodeID `yaml:"storagenode"`
}

type Redundancy struct {
	Algorithm      int `yaml:"algorithm"`
	ShareSize      int `yaml:"sharesize"`
	RequiredShares int `yaml:"requiredshares"`
	RepairShares   int `yaml:"repairshares"`
	OptimalShares  int `yaml:"optimalshares"`
	TotalShares    int `yaml:"totalshares"`
}

type SegmentFile struct {
	StreamID uuid.UUID `yaml:"streamid"`
	Position struct {
		Part  int `yaml:"part"`
		Index int `yaml:"index"`
	} `yaml:"position"`
	CreatedAt     time.Time     `yaml:"createdat"`
	ExpiresAt     *time.Time    `yaml:"expiresat"`
	RepairedAt    *time.Time    `yaml:"repairedat"`
	RootPieceID   storj.PieceID `yaml:"rootpieceid"`
	EncryptedSize int64         `yaml:"encryptedsize"`
	PlainOffset   int64         `yaml:"plainoffset"`
	PlainSize     int64         `yaml:"plainsize"`
	AliasPieces   []AliasPiece  `yaml:"aliaspieces"`
	Redundancy    Redundancy    `yaml:"redundancy"`
	Pieces        []Piece       `yaml:"pieces"`
	Placement     int           `yaml:"placement"`
	Source        string        `yaml:"source"`
}

// ReadSegmentFile reads and unmarshals a SegmentFile from the given path.
func ReadSegmentFile(path string) (metabase.Segment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return metabase.Segment{}, err
	}
	var seg SegmentFile
	if err := yaml.Unmarshal(data, &seg); err != nil {
		return metabase.Segment{}, err
	}
	return seg.ToMetabaseSegment(), nil
}

// ToMetabaseSegment converts a *SegmentFile to a metabase.Segment.
func (sf *SegmentFile) ToMetabaseSegment() metabase.Segment {
	return metabase.Segment{
		StreamID: sf.StreamID,
		Position: metabase.SegmentPosition{
			Part:  uint32(sf.Position.Part),
			Index: uint32(sf.Position.Index),
		},
		CreatedAt:     sf.CreatedAt,
		ExpiresAt:     sf.ExpiresAt,
		RepairedAt:    sf.RepairedAt,
		RootPieceID:   sf.RootPieceID,
		EncryptedSize: int32(sf.EncryptedSize),
		PlainOffset:   sf.PlainOffset,
		PlainSize:     int32(sf.PlainSize),
		Redundancy: storj.RedundancyScheme{
			//Algorithm:      sf.Redundancy.Algorithm,
			ShareSize:      int32(sf.Redundancy.ShareSize),
			RequiredShares: int16(sf.Redundancy.RequiredShares),
			RepairShares:   int16(sf.Redundancy.RepairShares),
			OptimalShares:  int16(sf.Redundancy.OptimalShares),
			TotalShares:    int16(sf.Redundancy.TotalShares),
		},
		Pieces:    convertPieces(sf.Pieces),
		Placement: storj.PlacementConstraint(sf.Placement),
	}
}

func convertAliasPieces(src []AliasPiece) []metabase.AliasPiece {
	res := make([]metabase.AliasPiece, len(src))
	//for i, ap := range src {
	//	res[i] = metabase.AliasPiece{
	//		Number: ap.Number,
	//		Alias:  ap.Alias,
	//	}
	//}
	return res
}

func convertPieces(src []Piece) []metabase.Piece {
	res := make([]metabase.Piece, len(src))
	for i, p := range src {
		res[i] = metabase.Piece{
			Number:      uint16(p.Number),
			StorageNode: p.StorageNode,
		}
	}
	return res
}
