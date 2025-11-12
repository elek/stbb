package db

type Db struct {
	Inline       Inline       `cmd:"" help:"inline segment load test"`
	GetObject    GetObject    `cmd:"" help:"get object from metabase by project ID, bucket and encrypted path"`
	GetBucket    GetBucket    `cmd:"" help:"get bucket from metabase by project ID and bucket name"`
	GetNode      GetNode      `cmd:"" help:"get node from satellite db by node ID"`
	ListProjects ListProjects `cmd:"" help:"List projects."`
	ApiKey       ApiKey       `cmd:"" help:"Print details of current api key (including project id)"`
	GetUser      GetUser      `cmd:"" help:"Print details of a user"`
	ListSegments ListSegments `cmd:"" help:"list segments for a given project ID and stream ID"`
}
