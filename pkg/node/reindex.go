package node

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/zeebo/errs/v2"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"time"
)

// run denormalizes piece information, creating piece->node list from metainfo DB.
func run() error {
	writers := make(map[metabase.NodeAlias]writer)
	defer func() {
		for _, w := range writers {
			_ = w.Close()
		}
	}()

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		return errs.Wrap(err)
	}
	defer conn.Close(context.Background())

	nodes, err := getNodeAliasMap(conn)
	if err != nil {
		return errs.Wrap(err)
	}

	rows, err := conn.Query(context.Background(), "select stream_id,position,root_piece_id,remote_alias_pieces from segments where inline_data is null")
	if err != nil {
		return errs.Wrap(err)
	}
	defer rows.Close()
	var streamID []byte
	var position int64
	var rootPieceID storj.PieceID
	var locations []byte
	alias := metabase.AliasPieces{}

	i := 0
	last := time.Now()
	for rows.Next() {
		err = rows.Scan(&streamID, &position, &rootPieceID, &locations)
		if err != nil {
			return errs.Wrap(err)
		}
		err := alias.SetBytes(locations)
		if err != nil {
			return errs.Wrap(err)
		}
		for _, r := range alias {
			//if r.Alias != 1 {
			//	continue
			//}
			if _, found := writers[r.Alias]; !found {
				nodeIDHex := hex.EncodeToString(nodes[r.Alias].Bytes())

				destFile := fmt.Sprintf("nodes/%s/%s", nodeIDHex[0:2], nodeIDHex)
				_ = os.MkdirAll(filepath.Dir(destFile), 0755)
				f, err := os.Create(destFile)
				if err != nil {
					return err
				}
				writers[r.Alias] = writer{
					output: f,
				}

			}
			derivedPieceID := rootPieceID.Deriver().Derive(nodes[r.Alias], int32(r.Number))
			err = writers[r.Alias].Write(streamID, position, rootPieceID, derivedPieceID, r.Number)
			if err != nil {
				return err
			}
		}

		i++
		if i%100 == 0 {
			elapsed := time.Since(last).Milliseconds()
			if elapsed > 1000 {
				fmt.Printf("%d/s\n", i*1000/int(elapsed))
				last = time.Now()
				i = 0
			}
		}
	}

	return nil
}

func getNodeAliasMap(conn *pgx.Conn) (map[metabase.NodeAlias]storj.NodeID, error) {
	res := make(map[metabase.NodeAlias]storj.NodeID)
	var alias metabase.NodeAlias
	var nodeIDBytes []byte

	rows, err := conn.Query(context.Background(), "select node_id, node_alias from node_aliases")
	if err != nil {
		return res, errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&nodeIDBytes, &alias)
		if err != nil {
			return res, errs.Wrap(err)
		}
		res[alias], err = storj.NodeIDFromBytes(nodeIDBytes)
		if err != nil {
			return res, errs.Wrap(err)
		}
	}
	return res, nil
}

type writer struct {
	output *os.File
}

func (w writer) Write(streamID []byte, position int64, rootPieceID storj.PieceID, piece storj.PieceID, ix uint16) error {
	_, err := w.output.Write([]byte(fmt.Sprintf("%s,%d,%s,%d\n", hex.EncodeToString(streamID), position, piece.String(), ix)))
	return err
}

func (w *writer) Close() error {
	return w.output.Close()
}
