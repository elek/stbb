package metabase

type Metabase struct {
	Generate Generate `cmd:"" usage:"Generate segments for test data"`
	Inline   Inline   `cmd:"" usage:"inline segment load test"`
}
