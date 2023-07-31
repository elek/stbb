package downloadng

import (
	"context"
	"fmt"
	"os"
	"storj.io/common/grant"
	"storj.io/common/storj"
	"sync"
)

type FatalFailure struct {
	Error error
}

type Done struct {
}

func download(bucket string, key string) error {
	access, err := grant.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	ctx := context.Background()

	first := make(chan any)
	downloader := ObjectDownloader{
		inbox:            first,
		outbox:           make(chan any),
		satelliteAddress: access.SatelliteAddress,
		APIKey:           access.APIKey,
		store:            access.EncAccess.Store,
	}

	sd := &DownloadRouter{
		inbox:       logReceived("DownloadRouter", downloader.outbox),
		outbox:      make(chan any),
		connections: make(map[storj.NodeID]chan any),
		factory: func(url storj.NodeURL, outbox chan any) (chan any, error) {
			client, err := NewPieceStoreClient(url, outbox)
			if err != nil {
				return nil, err
			}
			go client.Run(context.Background())
			return client.inbox, nil
		},
	}

	p := Parallel{
		global:   first,
		inbox:    logReceived("Parallel", sd.outbox),
		outbox:   make(chan any),
		segments: map[string]*segmentBuffer{},
	}

	ec, err := NewECDecoder(p.outbox)
	if err != nil {
		return err
	}

	dc, err := NewDecrypt(ec.outbox, access.EncAccess.Store)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(6)
	go func() {
		defer wg.Done()
		err := downloader.Run(ctx)
		if err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		defer wg.Done()
		err := dc.Run(ctx)
		if err != nil {
			fmt.Println("decryption is failed", err)
		}
	}()
	go func() {
		defer wg.Done()
		err := sd.Run(ctx)
		if err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		if err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		defer wg.Done()
		err := ec.Run(ctx)
		if err != nil {
			fmt.Println(err)
		}
	}()
	go func() {
		defer wg.Done()
		out, err := os.Create("/tmp/out")
		if err != nil {
			panic(err)
		}
		defer out.Close()

		for {
			select {
			case msg := <-dc.outbox:
				switch r := msg.(type) {
				case []byte:
					out.Write(r)
				case Done:
					return
				}

			}
		}
	}()

	downloader.inbox <- &DownloadObject{
		bucket: bucket,
		key:    key,
	}

	wg.Wait()
	return nil
}

func logReceived[T any](name string, outbox chan T) chan T {
	c := make(chan T)
	go func() {
		for {
			select {
			case t, ok := <-outbox:
				if !ok {
					//close(c)
					return
				}
				fmt.Printf("%s: %T\n", name, t)
				c <- t
			}
		}
	}()
	return c
}
