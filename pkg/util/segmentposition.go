package util

import (
	"encoding/hex"
	"fmt"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"strconv"
	"strings"
)

// ParseSegmentPosition parse segment position from segment/pos format
func ParseSegmentPosition(i string) (uuid.UUID, metabase.SegmentPosition, error) {
	sp := metabase.SegmentPosition{}
	parts := strings.Split(i, "/")

	if len(parts) > 1 {
		part, err := strconv.Atoi(parts[1])
		if err != nil {
			return uuid.UUID{}, metabase.SegmentPosition{}, err
		}
		sp = metabase.SegmentPositionFromEncoded(uint64(part))
	}
	su, err := ParseUUID(parts[0])
	if err != nil {
		return uuid.UUID{}, metabase.SegmentPosition{}, err
	}
	return su, sp, nil
}

func ParseUUID(id string) (uuid.UUID, error) {
	if id[0] == '#' {
		sid, _ := uuid.New()
		decoded, err := hex.DecodeString(id[1:])
		if err != nil {
			return uuid.UUID{}, err
		}
		copy(sid[:], decoded)
		fmt.Println(sid.String())
		return sid, nil
	}
	if !strings.Contains(id, "-") {
		id = id[0:8] + "-" + id[8:12] + "-" + id[12:16] + "-" + id[16:20] + "-" + id[20:]
		fmt.Println(id)
	}
	return uuid.FromString(id)
}
