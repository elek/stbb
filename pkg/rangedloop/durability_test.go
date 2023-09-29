package rangedloop

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"storj.io/common/identity/testidentity"
	"storj.io/common/storj"
	"storj.io/common/storj/location"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/nodeselection"
	"testing"
	"time"
)

func TestDurability(t *testing.T) {
	var storageNodes []*FullSelectedNode
	for i := 0; i < 10; i++ {
		storageNodes = append(storageNodes, &FullSelectedNode{
			SelectedNode: nodeselection.SelectedNode{
				ID:      testidentity.MustPregeneratedIdentity(i, storj.LatestIDVersion()).ID,
				LastNet: fmt.Sprintf("127.0.%d.0", i%3),
			},
		})
	}
	ctx := context.TODO()
	c := Durability{
		nodeGetter: SelectNodeFromList(storageNodes),
		healthStat: make(map[string]*HealthStat),
		groups: []GroupClassifier{
			func(node *FullSelectedNode) string {
				return "net:" + node.LastNet
			},
		},
	}
	err := c.Start(ctx, time.Now())
	require.NoError(t, err)

	{
		fork, err := c.Fork(ctx)
		require.NoError(t, err)

		err = fork.Process(ctx, []rangedloop.Segment{
			{
				StreamID: testUUID(t),
				Position: metabase.SegmentPosition{
					Part:  1,
					Index: 1,
				},
				Pieces: pieces(storageNodes, 3, 6, 9, 1),
			},
			{
				StreamID: testUUID(t),
				Position: metabase.SegmentPosition{
					Part:  1,
					Index: 1,
				},
				Pieces: pieces(storageNodes, 1, 2, 3, 4),
			},
		})
		require.NoError(t, err)

		err = c.Join(ctx, fork)
		require.NoError(t, err)
	}

	{
		fork, err := c.Fork(ctx)
		require.NoError(t, err)

		err = fork.Process(ctx, []rangedloop.Segment{
			{
				StreamID: testUUID(t),
				Position: metabase.SegmentPosition{
					Part:  1,
					Index: 1,
				},
				Pieces: pieces(storageNodes, 2, 3, 4, 7),
			},
			{
				StreamID: testUUID(t),
				Position: metabase.SegmentPosition{
					Part:  1,
					Index: 1,
				},
				Pieces: pieces(storageNodes, 1, 2, 3, 4, 6, 7, 8),
			},
		})
		require.NoError(t, err)

		err = c.Join(ctx, fork)
		require.NoError(t, err)
	}

	for n, c := range c.healthStat {
		fmt.Println(n, c.min)
	}

}

func pieces(nodes []*FullSelectedNode, ix ...int) (res metabase.Pieces) {
	for n, i := range ix {
		res = append(res, metabase.Piece{
			Number:      uint16(n),
			StorageNode: nodes[i].ID,
		})
	}
	return res
}

func BenchmarkProcess(b *testing.B) {
	ctx := context.TODO()

	var nodes []*FullSelectedNode
	for i := 0; i < 20000; i++ {
		identity, err := testidentity.NewTestIdentity(ctx)
		require.NoError(b, err)
		nodes = append(nodes, &FullSelectedNode{
			SelectedNode: nodeselection.SelectedNode{
				ID:          identity.ID,
				LastNet:     fmt.Sprintf(fmt.Sprintf("10.8.0.0")),
				CountryCode: location.UnitedStates,
			},
			Email: fmt.Sprintf("test+%d@asd.hu", i%2),
		})
	}
	fmt.Println("nodes are initialized")

	var segments []rangedloop.Segment
	for i := 0; i < 2500; i++ {
		id, err := uuid.New()
		require.NoError(b, err)

		var pieces metabase.Pieces
		for j := 0; j < 80; j++ {
			nodeIx := rand.Intn(len(nodes) - 1)
			pieces = append(pieces, metabase.Piece{
				Number:      uint16(j),
				StorageNode: nodes[nodeIx].ID,
			})
		}
		segments = append(segments, rangedloop.Segment{
			StreamID: id,
			Position: metabase.SegmentPosition{
				Part:  1,
				Index: 1,
			},
			CreatedAt: time.Now(),
			Pieces:    pieces,
		})
	}

	d := DurabilityFork{
		nodeGetter: SelectNodeFromListUsingMap(nodes),
		groups: []GroupClassifier{
			func(node *FullSelectedNode) string {
				return "email:" + node.Email
			},
		},
		healthStat: make(map[string]*HealthStat),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkProcess(ctx, b, d, segments)
	}
}

func benchmarkProcess(ctx context.Context, b *testing.B, d DurabilityFork, segments []rangedloop.Segment) {
	err := d.Process(ctx, segments)
	require.NoError(b, err)
}
