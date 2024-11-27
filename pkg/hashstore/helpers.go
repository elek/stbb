package hashstore

import "time"

//
// date/time helpers
//

// saturatingUint23 returns the uint32 value of x, saturating to the maximum if the conversion
// would overflow or underflow. This is used to put a maximum date on expiration times and so that
// if someone passes in an expiration way in the future it doesn't end up in the past.
func saturatingUint23(x int64) uint32 {
	if uint64(x) >= 1<<23-1 {
		return 1<<23 - 1
	}
	return uint32(x)
}

func timeToDateDown(t time.Time) uint32 { return saturatingUint23(t.Unix() / 86400) }
func timeToDateUp(t time.Time) uint32   { return saturatingUint23((t.Unix() + 86400 - 1) / 86400) }
func dateToTime(d uint32) time.Time     { return time.Unix(int64(d)*86400, 0).UTC() }

//
