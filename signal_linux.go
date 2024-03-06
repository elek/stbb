package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func initSignal() func() {
	usr1 := make(chan os.Signal, 1)

	signal.Notify(usr1, syscall.SIGUSR1)
	go func() {
		for {
			select {
			case _, ok := <-usr1:
				if !ok {
					return
				}
				fmt.Println(string(readStack()))
			}
		}
	}()
	return func() {
		close(usr1)
	}
}
