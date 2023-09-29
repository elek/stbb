package rangedloop

import (
	"database/sql/driver"
	"encoding/binary"
	"github.com/zeebo/errs"
	"storj.io/common/storj"
)

var Error = errs.Class("rs")

type redundancyScheme struct {
	*storj.RedundancyScheme
}

// Check that RedundancyScheme layout doesn't change.
var _ struct {
	Algorithm      storj.RedundancyAlgorithm
	ShareSize      int32
	RequiredShares int16
	RepairShares   int16
	OptimalShares  int16
	TotalShares    int16
} = storj.RedundancyScheme{}

func (params redundancyScheme) Value() (driver.Value, error) {
	switch {
	case params.ShareSize < 0 || params.ShareSize >= 1<<24:
		return nil, Error.New("invalid share size %v", params.ShareSize)
	case params.RequiredShares < 0 || params.RequiredShares >= 1<<8:
		return nil, Error.New("invalid required shares %v", params.RequiredShares)
	case params.RepairShares < 0 || params.RepairShares >= 1<<8:
		return nil, Error.New("invalid repair shares %v", params.RepairShares)
	case params.OptimalShares < 0 || params.OptimalShares >= 1<<8:
		return nil, Error.New("invalid optimal shares %v", params.OptimalShares)
	case params.TotalShares < 0 || params.TotalShares >= 1<<8:
		return nil, Error.New("invalid total shares %v", params.TotalShares)
	}

	var bytes [8]byte
	bytes[0] = byte(params.Algorithm)

	// little endian uint32
	bytes[1] = byte(params.ShareSize >> 0)
	bytes[2] = byte(params.ShareSize >> 8)
	bytes[3] = byte(params.ShareSize >> 16)

	bytes[4] = byte(params.RequiredShares)
	bytes[5] = byte(params.RepairShares)
	bytes[6] = byte(params.OptimalShares)
	bytes[7] = byte(params.TotalShares)

	return int64(binary.LittleEndian.Uint64(bytes[:])), nil
}

func (params redundancyScheme) Scan(value interface{}) error {
	switch value := value.(type) {
	case int64:
		var bytes [8]byte
		binary.LittleEndian.PutUint64(bytes[:], uint64(value))

		params.Algorithm = storj.RedundancyAlgorithm(bytes[0])

		// little endian uint32
		params.ShareSize = int32(bytes[1]) | int32(bytes[2])<<8 | int32(bytes[3])<<16

		params.RequiredShares = int16(bytes[4])
		params.RepairShares = int16(bytes[5])
		params.OptimalShares = int16(bytes[6])
		params.TotalShares = int16(bytes[7])

		return nil
	default:
		return Error.New("unable to scan %T into RedundancyScheme", value)
	}
}
