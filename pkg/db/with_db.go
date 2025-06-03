package db

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/satellitedb"
)

type WithDatabase struct {
	Satellite  string
	Satellites map[string]SatelliteConfig
}

type SatelliteConfig struct {
	Satellite string
	Metabase  string
}

func (w WithDatabase) GetSatelliteDB(ctx context.Context, log *zap.Logger) (satellite.DB, error) {
	url := os.Getenv("STBB_DB_SATELLITE")
	if url == "" && w.Satellite != "" {
		config, found := w.Satellites[w.Satellite]
		if !found {
			return nil, errors.New("satellite configuration is not found")
		}
		url = config.Satellite
	}
	satelliteDB, err := satellitedb.Open(ctx, log.Named("satellite"), url, satellitedb.Options{
		ApplicationName: "stbb",
	})
	return satelliteDB, err
}

func (w WithDatabase) GetMetabaseDB(ctx context.Context, log *zap.Logger) (*metabase.DB, error) {
	url := os.Getenv("STBB_DB_METABASE")
	if url == "" && w.Satellite != "" {
		config, found := w.Satellites[w.Satellite]
		if !found {
			return nil, errors.New("satellite configuration is not found")
		}
		url = config.Metabase
	}
	db, err := metabase.Open(ctx, log.Named("metabase"), url, metabase.Config{
		ApplicationName: "stbb",
	})
	return db, err
}
