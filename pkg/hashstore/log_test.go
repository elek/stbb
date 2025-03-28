package hashstore

import (
	"github.com/stretchr/testify/require"
	"storj.io/storj/storagenode/hashstore"
	"strconv"
	"testing"
)

func TestDataPase(t *testing.T) {
	name := "log-00000000000022ed-00004ecc"

	ttlTime, err := strconv.ParseUint(name[21:], 16, 64)
	require.NoError(t, err)
	require.Equal(t, "2025-03-25", hashstore.DateToTime(uint32(ttlTime)).Format("2006-01-02"))

	name = "log-00000000000022ed-00000000"
	ttlTime, err = strconv.ParseUint(name[21:], 16, 64)
	require.NoError(t, err)
	require.True(t, hashstore.DateToTime(uint32(ttlTime)).IsZero())
}
