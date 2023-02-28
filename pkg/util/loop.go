package util

import (
	"fmt"
	"time"
)

func Loop(n int, verbose bool, do func() error) (durationMs int64, err error) {
	for i := 0; i < n; i++ {
		start := time.Now()
		err = do()
		if err != nil {
			return
		}
		elapsed := time.Since(start)
		if verbose {
			fmt.Println(elapsed)
		}
		durationMs += elapsed.Milliseconds()
	}
	fmt.Printf("Executed %d times during %d ms (average: %f ms) %f req/sec", n, durationMs, float64(durationMs)/float64(n), float64(n*1000)/float64(durationMs))
	return
}
