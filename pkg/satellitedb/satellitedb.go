package satellitedb

type SatelliteDB struct {
	ListProjects ListProjects `cmd:"" help:"List projects."`
	ApiKey       ApiKey       `cmd:"" help:"Print details of current api key (including project id)"`
}
