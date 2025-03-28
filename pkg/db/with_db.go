package db

import (
	"context"
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
		url = w.Satellites[w.Satellite].Satellite
	}
	satelliteDB, err := satellitedb.Open(ctx, log.Named("satellite"), url, satellitedb.Options{
		ApplicationName: "stbb",
	})
	return satelliteDB, err
}

func (w WithDatabase) GetMetabaseDB(ctx context.Context, log *zap.Logger) (*metabase.DB, error) {
	url := os.Getenv("STBB_DB_METABASE")
	if url == "" && w.Satellite != "" {
		url = w.Satellites[w.Satellite].Metabase
	}
	db, err := metabase.Open(ctx, log.Named("metabase"), url, metabase.Config{
		ApplicationName: "stbb",
	})
	return db, err
}
