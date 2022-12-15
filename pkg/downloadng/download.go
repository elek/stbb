package downloadng

import (
	"context"
	"fmt"
	"os"
	"storj.io/common/grant"
	"storj.io/common/storj"
	"sync"
)

func download(bucket []byte, key []byte) error {
	access, err := grant.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	ctx := context.Background()

	downloader := ObjectDownloader{
		inbox:            make(chan *DownloadObject),
		outboxDownload:   logSent(make(chan *DownloadPiece)),
		satelliteAddress: access.SatelliteAddress,
		APIKey:           access.APIKey,
	}

	result := make(chan *Download)
	sd := NewDownloadRouter(downloader.outboxDownload, func(url storj.NodeURL) (chan *DownloadPiece, error) {
		client, err := NewPieceStoreClient(url, result)
		if err != nil {
			return nil, err
		}
		go client.Run(context.Background())
		return client.Inbox(), nil
	})

	p := NewParallel(result, downloader)

	ec, err := NewECDecoder(p.Outbox())
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer wg.Done()
		err := downloader.Run(ctx)
		if err != nil {
			fmt.Println(err)
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

	downloader.inbox <- &DownloadObject{
		bucket:       bucket,
		encryptedKey: key,
	}

	wg.Wait()
	return nil
}

func logReceived[T any](outbox chan T) chan T {
	c := make(chan T)
	go func() {
		for {
			select {
			case t, ok := <-outbox:
				if !ok {
					close(c)
					return
				}
				fmt.Println(t)
				c <- t
			}
		}
	}()
	return c
}
func logSent[T any](outbox chan T) chan T {
	c := make(chan T)
	go func() {
		for {
			select {
			case t, ok := <-c:
				if !ok {
					close(outbox)
					return
				}
				fmt.Println(t)
				outbox <- t
			}
		}
	}()
	return c
}

func simulatedDownloader(downloader ObjectDownloader) {
	for {
		select {
		case out := <-downloader.outboxDownload:
			if out == nil {
				return
			}
			fmt.Println(out.sn)
		}
	}
}

func simulatedSegmentDownloader(node storj.NodeURL) (chan *DownloadPiece, error) {
	c := make(chan *DownloadPiece)
	go func() {
		for {
			select {
			case task := <-c:
				if task == nil {
					return
				}
				fmt.Println("Download " + task.orderLimit.String())
			}
		}
	}()
	return c, nil
}
