package util

import (
	"fmt"
	"sync"
	"time"
)

type Progres struct {
	counter     int64
	lastCounter int64
	last        time.Time
	mu          sync.Mutex
}

func (o *Progres) Increment() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counter++
	o.lastCounter++
	if o.counter%10000 == 0 || time.Since(o.last) > 6*time.Second {
		fmt.Printf("%d %d/sec\n", o.counter, int(float64(o.lastCounter)/time.Since(o.last).Seconds()))
		o.last = time.Now()
		o.lastCounter = 0
	}
}

func (o *Progres) Counter() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.counter
}
