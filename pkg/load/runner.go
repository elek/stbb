package load

import (
	"context"
	"fmt"
	"storj.io/common/storj"
	"sync"
)

type Runner struct {
	Workers int `default:"1"`
	Limit   int `default:"1"`
	PieceIDStream
}

func (p Runner) RunTest(test func(ctx context.Context, p storj.PieceID) error) {
	ctx := context.Background()

	var uwg sync.WaitGroup

	pieceIDQueue := make(chan storj.PieceID, p.Workers)

	for i := 0; i < p.Workers; i++ {
		uwg.Add(1)
		go func() {
			defer uwg.Done()
			for pieceID := range pieceIDQueue {
				if pieceID.IsZero() {
					return
				}
				if err := test(ctx, pieceID); err != nil {
					fmt.Println(err)
				}
			}
		}()
	}

	allPieceIds := make([]storj.PieceID, 0, p.Limit)
	for i := 0; i < p.Limit; i++ {
		allPieceIds = append(allPieceIds, p.NextPieceID())
	}

	for _, pieceID := range allPieceIds {
		pieceIDQueue <- pieceID
	}

	for i := 0; i < p.Workers; i++ {
		pieceIDQueue <- storj.PieceID{}
	}
	uwg.Wait()
}
