package audit

type Audit struct {
	Decode Decode `cmd:"decode" help:"decode audit history binary"`
}
