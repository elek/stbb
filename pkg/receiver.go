package stbb

import (
	"context"
	"fmt"
	"storj.io/common/telemetry"
)

type TelemetryReceiver struct {
	Address string `arg:"" default:"localhost:9000"`
}

func (t TelemetryReceiver) Run() error {
	listen, err := telemetry.Listen(t.Address)
	if err != nil {
		return err
	}

	return listen.Serve(context.Background(), telemetry.HandlerFunc(func(application, instance string, key []byte, val float64) {
		fmt.Println(application, instance, string(key), val)
	}))
}
