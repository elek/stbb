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
		outbox:           make(chan *DownloadPiece),
		satelliteAddress: access.SatelliteAddress,
		APIKey:           access.APIKey,
	}

	result := make(chan *Download)
	sd := NewSegmentDownloader(downloader.outbox, func(url storj.NodeURL) (chan *DownloadPiece, error) {
		client, err := NewPieceStoreClient(url, result)
		if err != nil {
			return nil, err
		}
		go client.Run(context.Background())
		return client.Inbox(), nil
	})

	ec := ECDecoder{
		inbox:    result,
		segments: map[string]*segmentBuffer{},
		finish: func() {
			close(downloader.inbox)
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(3)
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

func simulatedDownloader(downloader ObjectDownloader) {
	for {
		select {
		case out := <-downloader.outbox:
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
