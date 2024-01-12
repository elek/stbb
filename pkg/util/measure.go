package util

import "time"

func Measure(f func() error) (time.Duration, error) {
	start := time.Now()
	err := f()
	return time.Since(start), err
}
