package metabase

type Metabase struct {
	Inline    Inline    `cmd:"" help:"inline segment load test"`
	GetObject GetObject `cmd:"" help:"get object from metabase by project ID, bucket and encrypted path"`
	GetBucket GetBucket `cmd:"" help:"get bucket from metabase by project ID and bucket name"`
}
