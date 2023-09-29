package rangedloop

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"storj.io/common/storj"
)

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
	Placement int
}

type Object struct {
	StreamID     []byte
	PieceAliases []byte
	ObjectKey    []byte
}

func segmentsOfUser(satelliteConn *pgx.Conn, email string, f func(o Segment) error) error {
	return forEachUser(satelliteConn, email, func(userID []byte) error {
		return forEachProject(satelliteConn, userID, func(projectID []byte, projectName string) error {
			return forEachBucket(satelliteConn, projectID, func(bucketID []byte, bucketName []byte) error {
				return forEachSegment(satelliteConn, projectID, bucketName, f)
			})
		})
	})
}

func segmentsOfPlacement(satelliteConn *pgx.Conn, placement storj.PlacementConstraint, f func(o Segment) error) error {
	rows, err := satelliteConn.Query(context.Background(), "select segments.stream_id,segments.remote_alias_pieces,segments.position FROM segments WHERE segments.position is not null AND segments.remote_alias_pieces is not null  AND segments.placement=$1", placement)
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
		err = f(p)
		if err != nil {
			return errors.WithStack(err)
		}

	}
	return nil
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
