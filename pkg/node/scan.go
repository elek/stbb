package node

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/elek/stbb/pkg/piece"
	"github.com/elek/stbb/pkg/util"
	"os"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"sync"
	"time"
)

type result struct {
	duration time.Duration
	nodeInfo storj.NodeURL
	err      error
	action   string
}

type Scan struct {
	util.DialerHelper
	NodeFile     string `arg:""`
	FileToUpload string `arg:""`
	Action       string `default:"upload"`
	Keys         string
}

func (s Scan) Run() error {
	ctx := context.Background()
	tasks := make(chan storj.NodeURL)
	results := make(chan result)
	wg := sync.WaitGroup{}

	for i := 0; i < 50; i++ {
		go func() {

			for {
				select {
				case task := <-tasks:
					switch s.Action {
					case "list":
						results <- result{
							duration: 0,
							err:      nil,
							nodeInfo: task,
							action:   "list",
						}
						wg.Done()
					case "upload":

						orderLimitCreator, err := piece.NewKeySignerFromDir(s.Keys)
						if err != nil {
							panic(err)
						}
						orderLimitCreator.Action = pb.PieceAction_PUT

						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						duration, err := measure(func() error {
							upload := piece.UploadDrpc{
								DialerHelper: s.DialerHelper,
								Keys:         s.Keys,
								NodeURL:      task,
								File:         s.FileToUpload,
							}
							_, _, err = upload.ConnectAndUpload(ctx, orderLimitCreator)
							return err
						})
						results <- result{
							duration: duration,
							err:      err,
							nodeInfo: task,
							action:   "upload",
						}
						wg.Done()
					case "updown":

						orderLimitCreator, err := piece.NewKeySignerFromDir(s.Keys)
						if err != nil {
							panic(err)
						}
						orderLimitCreator.Action = pb.PieceAction_PUT
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						var size int
						var pieceID storj.PieceID
						duration, err := measure(func() error {
							upload := piece.UploadDrpc{
								DialerHelper: s.DialerHelper,
								Keys:         s.Keys,
								NodeURL:      task,
								File:         s.FileToUpload,
							}
							size, pieceID, err = upload.ConnectAndUpload(ctx, orderLimitCreator)
							return err
						})
						results <- result{
							duration: duration,
							err:      err,
							nodeInfo: task,
							action:   "upload",
						}

						if err == nil {
							signer, err := piece.NewKeySignerFromDir(s.Keys)
							if err != nil {
								panic(err)
							}
							signer.Action = pb.PieceAction_GET
							duration, err := measure(func() error {
								download := piece.DownloadDRPC{
									DialerHelper: s.DialerHelper,
									Keys:         s.Keys,
									NodeURL:      task,
									Size:         int64(size),
									Piece:        pieceID.String(),
								}
								err = download.ConnectAndDownload(ctx, signer)
								return err
							})
							results <- result{
								duration: duration,
								err:      err,
								nodeInfo: task,
								action:   "download",
							}
						}
						wg.Done()
					default:
						panic("Unknown action " + s.Action)
					}
				}

			}
		}()
	}

	wgWrite := sync.WaitGroup{}
	wgWrite.Add(1)
	go func() {
		defer wgWrite.Done()
		out := csv.NewWriter(os.Stdout)
		defer out.Flush()
		err := out.Write([]string{
			"node",
			"duration",
			"action",
			"protocol",
			"error",
		})
		if err != nil {
			panic(err)
		}
		for {
			select {
			case res, ok := <-results:
				if !ok {
					return
				}
				if res.action == "done" {
					return
				}
				e := ""
				if res.err != nil {
					res.duration = -1
					e = res.err.Error()
				}
				err := out.Write([]string{
					res.nodeInfo.ID.String(),
					fmt.Sprintf("%d", res.duration.Milliseconds()),
					res.action,
					e,
				})
				if err != nil {
					panic(err)
				}
			}
		}

	}()
	err := forEachNode(s.NodeFile, func(node storj.NodeURL, _ map[string]string) error {
		wg.Add(1)
		tasks <- node
		return nil
	})
	if err != nil {
		panic(err)
	}
	wg.Wait()
	results <- result{
		action: "done",
	}
	wgWrite.Wait()
	return err
}
