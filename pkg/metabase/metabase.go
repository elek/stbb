package metabase

type Metabase struct {
	Generate Generate `cmd:"" help:"Generate segments for test data"`
	Inline   Inline   `cmd:"" help:"inline segment load test"`
}
