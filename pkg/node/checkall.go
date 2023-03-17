package node

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/elek/stbb/pkg/piece"
	"github.com/elek/stbb/pkg/util"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"strings"
	"sync"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check retrievability of all pieces",
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return checkAll(args[0], args[1])
	}
	NodeCmd.AddCommand(cmd)
}

type Task struct {
	streamID    []byte
	position    int64 // segment position
	rootPieceID storj.PieceID
	node        storj.NodeID
	address     string // RS index
	number      uint16
}

type TaskResult struct {
	Task           Task
	connectionErr  error
	downloadErr    error
	derivedPieceID storj.PieceID
	duration       time.Duration
}

func checkAll(satelliteDB string, metainfoDB string) error {

	metainfoConn, err := pgx.Connect(context.Background(), metainfoDB)
	if err != nil {
		return errs.Wrap(err)
	}
	defer metainfoConn.Close(context.Background())

	satelliteConn, err := pgx.Connect(context.Background(), satelliteDB)
	if err != nil {
		return errs.Wrap(err)
	}
	defer satelliteConn.Close(context.Background())

	nodeAliases, err := getNodeAliasMap(metainfoConn)
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Printf("Loaded %d node alias\n", len(nodeAliases))

	nodeAddresses, err := getNodeAddresses(satelliteConn)
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Printf("Loaded %d node address\n", len(nodeAddresses))

	rows, err := metainfoConn.Query(context.Background(), "select stream_id,position,root_piece_id,remote_alias_pieces from segments where inline_data is null")
	if err != nil {
		return errs.Wrap(err)
	}
	defer func() {
		fmt.Println("closing row")
		rows.Close()
		fmt.Println("closed")
	}()
	var streamID []byte
	var position int64
	var rootPieceIDBytes []byte
	var rootPieceID storj.PieceID
	var locations []byte
	alias := metabase.AliasPieces{}

	results := make(chan TaskResult, 200)
	wg := sync.WaitGroup{}

	start := time.Now()

	go func() {
		counter := 0
		sinceLast := 0
		lastReport := time.Now()
		output, err := os.Create("results.csv")
		if err != nil {
			panic(err)
		}
		defer output.Close()
		c := csv.NewWriter(output)
		defer c.Flush()

		handle := func(result TaskResult) {
			if result.downloadErr != nil {
				err = c.Write([]string{
					result.Task.node.String(),
					hex.EncodeToString(result.Task.streamID),
					fmt.Sprintf("%d", result.Task.number),
					result.Task.rootPieceID.String(),
					result.downloadErr.Error(),
				})
				if err != nil {
					panic(err)
				}
			}
			counter++
			sinceLast++
			if counter%1000 == 0 && time.Since(lastReport).Milliseconds() > 1000 {
				fmt.Println(counter, sinceLast*1000/int(time.Since(lastReport).Milliseconds()), counter*1000/int(time.Since(start).Milliseconds()))
				lastReport = time.Now()
				sinceLast = 0
			}
		}
		for {
			select {
			case result, ok := <-results:
				if !ok {
					return
				}
				handle(result)
				wg.Done()

			}
		}
	}()

	workers := map[storj.NodeID]chan Task{}
	c := 0
	for rows.Next() {
		if rows.Err() != nil {
			break
		}
		err = rows.Scan(&streamID, &position, &rootPieceIDBytes, &locations)
		if err != nil {
			return errs.Wrap(err)
		}
		copy(rootPieceID[:], rootPieceIDBytes)
		err := alias.SetBytes(locations)
		if err != nil {
			return errs.Wrap(err)
		}
		for _, r := range alias {
			task := Task{
				streamID:    streamID,
				position:    position,
				number:      r.Number,
				rootPieceID: rootPieceID,
				node:        nodeAliases[r.Alias],
				address:     nodeAddresses[nodeAliases[r.Alias]],
			}
			wg.Add(1)
			worker, found := workers[task.node]
			if !found {
				workers[task.node] = createWorker(task.node, task.address, results)
				worker = workers[task.node]
			}
			worker <- task
			c++
		}

	}

	wg.Wait()
	for _, worker := range workers {
		close(worker)
	}
	close(results)
	return nil
}

func createWorker(node storj.NodeID, address string, results chan TaskResult) chan Task {
	fmt.Println("create worker", address)
	tasks := make(chan Task, 100)
	go func() {
		connectionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		d, connectionError := piece.NewDRPCDownloader(connectionCtx, node.String()+"@"+address, &util.DialerHelper{})
		defer d.Close()
		for {
			select {
			case task, ok := <-tasks:
				if !ok {
					return
				}
				start := time.Now()
				derivedPieceID := task.rootPieceID.Deriver().Derive(task.node, int32(task.number))
				if connectionError != nil {
					t := TaskResult{
						connectionErr:  connectionError,
						Task:           task,
						derivedPieceID: derivedPieceID,
					}
					results <- t
					continue
				}

				downloadCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _, err := d.Download(downloadCtx, derivedPieceID.String(), 256, func(bytes []byte) {})
				cancel()
				if errors.Is(err, context.Canceled) {
					connectionError = err
					err = nil
				}
				t := TaskResult{
					connectionErr:  connectionError,
					downloadErr:    err,
					Task:           task,
					derivedPieceID: derivedPieceID,
					duration:       time.Since(start),
				}
				results <- t
			}
		}
	}()
	return tasks
}
func persistentErr(err error) bool {
	if err == nil {
		return false
	}
	e := err.Error()
	if strings.Contains(e, "no route to host") {
		return true
	}
	if strings.Contains(e, "i/o timeout") {
		return true
	}
	if strings.Contains(e, "connection refused") {
		return true
	}
	if strings.Contains(e, "no such host") {
		return true
	}
	if strings.Contains(e, "peer ID did not") {
		return true
	}
	return false
}

func getNodeAddresses(conn *pgx.Conn) (map[storj.NodeID]string, error) {
	res := make(map[storj.NodeID]string)
	var address string
	var nodeIDBytes []byte
	rows, err := conn.Query(context.Background(), "select id,address from nodes")
	if err != nil {
		return res, errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&nodeIDBytes, &address)
		if err != nil {
			return res, errs.Wrap(err)
		}
		nodeID, err := storj.NodeIDFromBytes(nodeIDBytes)
		if err != nil {
			return res, err
		}
		res[nodeID] = address
	}
	return res, nil
}
