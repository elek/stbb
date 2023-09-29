package rangedloop

import (
	"context"
	"github.com/stretchr/testify/require"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"testing"
	"time"
)

func TestCount(t *testing.T) {
	ctx := context.TODO()
	c := Count{}
	err := c.Start(ctx, time.Now())
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		fork, err := c.Fork(ctx)
		require.NoError(t, err)

		err = fork.Process(ctx, []rangedloop.Segment{
			{
				StreamID: testUUID(t),
				Position: metabase.SegmentPosition{
					Part:  1,
					Index: 1,
				},
			},
			{
				StreamID: testUUID(t),
				Position: metabase.SegmentPosition{
					Part:  1,
					Index: 1,
				},
			},
		})
		require.NoError(t, err)

		err = c.Join(ctx, fork)
		require.NoError(t, err)
	}

	require.Equal(t, 20, c.count)

}

func testUUID(t *testing.T) uuid.UUID {
	id, err := uuid.New()
	require.NoError(t, err)
	return id
}
