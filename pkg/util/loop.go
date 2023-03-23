package util

import (
	"fmt"
	"time"
)

type Loop struct {
	Verbose bool `short:"v" help:"Print out more information"`
	Sample  int  `short:"n"  default:"1" help:"Number of executions"`
}

func (l *Loop) Run(do func() error) (durationMs int64, err error) {
	return RunLoop(l.Sample, l.Verbose, do)
}

func RunLoop(n int, verbose bool, do func() error) (durationMs int64, err error) {
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
