package placement

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/zeebo/errs/v2"
	"os"
	"storj.io/common/storj"
	"storj.io/common/storj/location"
	"storj.io/storj/satellite/metabase"
	"time"
)

type Placement struct {
	Check Check `cmd:""`
}
type Check struct {
	MaxmindDB string
}

func (c Check) Run() error {
	writers := make(map[metabase.NodeAlias]writer)
	defer func() {
		for _, w := range writers {
			_ = w.Close()
		}
	}()

	satelliteConn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL_SATELLITE"))
	if err != nil {
		return errs.Wrap(err)
	}
	defer satelliteConn.Close(context.Background())

	metainfoConn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL_METAINFO"))
	if err != nil {
		return errs.Wrap(err)
	}
	defer metainfoConn.Close(context.Background())

	nodeAliases, err := getNodeAliasMap(metainfoConn)
	if err != nil {
		return errs.Wrap(err)
	}

	nodes, err := getNodes(satelliteConn)
	if err != nil {
		return errs.Wrap(err)
	}

	aliases := metabase.AliasPieces{}

	err = forEachUser(satelliteConn, "test@storj.io", func(userID []byte) error {
		return forEachProject(satelliteConn, userID, func(projectID []byte, projectName string) error {
			return forEachBucket(satelliteConn, projectID, func(bucketID []byte, bucketName []byte) error {
				oc := 0
				err := forEachSegment(metainfoConn, projectID, bucketName, func(o Segment) error {
					oc++
					err := aliases.SetBytes(o.NodeAlias)
					if err != nil {
						return err
					}
					for _, n := range aliases {
						nodeID, found := nodeAliases[n.Alias]
						if !found {
							panic("Node not found")
						}
						node, found := nodes[nodeID]
						if !found {
							panic("Node not found")
						}
						if node.Country == location.Russia && time.Since(node.UpdatedAt) < 24*time.Hour {
							fmt.Printf("Pieces are at the wrong place: projectID=%x bucket=%x streamID=%x pos=%d aliasix=%d alias=%d nodeID=%s (%x) country=%x\n", projectID, bucketName, o.StreamID, o.Position, n.Number, n.Alias, node.NodeID, node.NodeID.Bytes(), node.Country)
						}
					}

					return nil
				})
				fmt.Printf("Bucket is checked: %x %x %d\n", projectID, bucketName, oc)
				return err
			})
		})

	})
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func forEachUser(satelliteConn *pgx.Conn, s string, f func(id []byte) error) error {
	users := make([][]byte, 0)
	rows, err := satelliteConn.Query(context.Background(), "select id FROM users where email = $1", s)
	if err != nil {
		return errs.Wrap(err)
	}
	defer rows.Close()
	var userID []byte
	for rows.Next() {
		err := rows.Scan(&userID)
		if err != nil {
			return errors.WithStack(err)
		}
		users = append(users, userID)
	}
	for _, id := range users {
		err := f(id)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type Project struct {
	ID   []byte
	Name string
}

type Bucket struct {
	ID   []byte
	Name []byte
}

type Segment struct {
	StreamID  []byte
	NodeAlias []byte
	Position  int
}

type Object struct {
	StreamID     []byte
	PieceAliases []byte
	ObjectKey    []byte
}

func forEachSegment(satelliteConn *pgx.Conn, projectID []byte, bucketName []byte, f func(o Segment) error) error {
	var res []Segment
	rows, err := satelliteConn.Query(context.Background(), "select segments.stream_id,segments.remote_alias_pieces,segments.position FROM objects left join segments on objects.stream_id = segments.stream_id where project_id = $1 AND bucket_name=$2 AND segments.position is not null AND segments.remote_alias_pieces is not null", projectID, bucketName)
	if err != nil {
		return errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		p := Segment{}
		err := rows.Scan(&p.StreamID, &p.NodeAlias, &p.Position)
		if err != nil {
			return errors.WithStack(err)
		}
		res = append(res, p)
	}
	for _, o := range res {
		err = f(o)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func forEachObject(satelliteConn *pgx.Conn, projectID []byte, bucketName []byte, f func(o Object) error) error {
	var res []Object
	rows, err := satelliteConn.Query(context.Background(), "select object_key,stream_id FROM objects where project_id = $1 AND bucket_name=$2", projectID, bucketName)
	if err != nil {
		return errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		p := Object{}
		err := rows.Scan(&p.ObjectKey, &p.StreamID)
		if err != nil {
			return errors.WithStack(err)
		}
		res = append(res, p)
	}
	for _, o := range res {
		err = f(o)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func forEachBucket(satelliteConn *pgx.Conn, projectID []byte, f func(id []byte, name []byte) error) error {
	var res []Bucket
	rows, err := satelliteConn.Query(context.Background(), "select id,name FROM bucket_metainfos where project_id = $1", projectID)
	if err != nil {
		return errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		p := Bucket{}
		err := rows.Scan(&p.ID, &p.Name)
		if err != nil {
			return errors.WithStack(err)
		}
		res = append(res, p)
	}
	for _, b := range res {
		err = f(b.ID, b.Name)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func forEachProject(satelliteConn *pgx.Conn, userID []byte, f func(id []byte, name string) error) error {
	var res []Project
	rows, err := satelliteConn.Query(context.Background(), "select id,name FROM projects where owner_id = $1", userID)
	if err != nil {
		return errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		p := Project{}
		err := rows.Scan(&p.ID, &p.Name)
		if err != nil {
			return errors.WithStack(err)
		}
		res = append(res, p)
	}
	for _, p := range res {
		err = f(p.ID, p.Name)
		if err != nil {
			return errors.WithStack(err)
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

type Node struct {
	NodeID    storj.NodeID
	Address   string
	Country   location.CountryCode
	UpdatedAt time.Time
}

func getNodes(conn *pgx.Conn) (map[storj.NodeID]Node, error) {
	res := make(map[storj.NodeID]Node)

	rows, err := conn.Query(context.Background(), "select id,address,country_code,updated_at from nodes")
	if err != nil {
		return res, errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		n := Node{}
		err := rows.Scan(&n.NodeID, &n.Address, &n.Country, &n.UpdatedAt)
		if err != nil {
			return res, errs.Wrap(err)
		}
		res[n.NodeID] = n
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
