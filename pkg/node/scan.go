package node

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/elek/stbb/pkg/piece"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"io"
	"os"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"sync"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "scan <nodes.csv> <upload|download> <file>",
		Short: "Test performance of each nodes, one bye one",
	}

	useQuic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic protocol")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return scanNodes(args[0], args[1], args[2], *useQuic)
	}
	NodeCmd.AddCommand(cmd)
}

type result struct {
	duration time.Duration
	nodeInfo NodeInfo
	err      error
}

func scanNodes(nodesFile string, action string, file string, quic bool) error {
	ctx := context.Background()
	tasks := make(chan NodeInfo)
	results := make(chan result)
	wg := sync.WaitGroup{}
	p := "tcp"
	if quic {
		p = "quic"
	}
	for i := 0; i < 50; i++ {
		go func() {
			for {
				select {
				case task := <-tasks:
					measured, err := measure(func() error {
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()

						switch action {
						case "upload":
							um, err := piece.NewDRPCUploader(ctx, task.NodeID.String()+"@"+task.Address, quic, pb.PieceHashAlgorithm_SHA256, false)
							if err != nil {
								return err
							}
							_, _, err = um.Upload(ctx, file)
							if err != nil {
								return err
							}
						default:
							panic("Unknown action " + action)
						}

						return nil
					})
					results <- result{
						duration: measured,
						err:      err,
						nodeInfo: task,
					}
					wg.Done()
				}
			}
		}()
	}
	go func() {
		out := csv.NewWriter(os.Stdout)
		defer out.Flush()
		err := out.Write([]string{
			"node",
			"duration",
			"action",
			"protocol",
			"file",
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
				e := ""
				if res.err != nil {
					res.duration = -1
					e = res.err.Error()
				}

				err := out.Write([]string{
					res.nodeInfo.NodeID.String(),
					fmt.Sprintf("%d", res.duration.Milliseconds()),
					action,
					p,
					file,
					e,
				})
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	err := forEachNode(nodesFile, func(node NodeInfo) error {
		wg.Add(1)
		tasks <- node
		return nil
	})
	wg.Wait()
	close(results)
	return err
}

func measure(f func() error) (time.Duration, error) {
	start := time.Now()
	err := f()
	return time.Since(start), err
}

func forEachNode(file string, cb func(node NodeInfo) error) error {
	input, err := os.Open(file)
	if err != nil {
		return errs.Wrap(err)
	}
	defer input.Close()
	nodes := csv.NewReader(input)
	headers := map[string]int{}
	for {
		record, err := nodes.Read()
		if errors.Is(io.EOF, err) {
			break
		}
		if err != nil {
			return err
		}
		if len(headers) == 0 {
			for i, r := range record {
				headers[r] = i
			}
			continue
		}

		idBytes, err := hex.DecodeString(record[headers["id"]])
		if err != nil {
			return errs.Wrap(err)
		}

		nodeID, err := storj.NodeIDFromBytes(idBytes)
		if err != nil {
			return errs.Wrap(err)
		}

		n := NodeInfo{
			NodeID:  nodeID,
			Address: record[headers["address"]],
			LastNet: record[headers["last_net"]],
		}
		err = cb(n)
		if err != nil {
			fmt.Println(nodeID, err)
		}
	}
	return nil
}

type NodeInfo struct {
	NodeID  storj.NodeID
	Address string
	LastNet string
}
