package audit

type Audit struct {
	Decode Decode `cmd:"decode" usage:"decode audit history binary"`
}
