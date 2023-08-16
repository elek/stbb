package load

type Load struct {
	Uplink Uplink `cmd:"" help:"Load generator with uplink upload/download"`
	Stat   Stat   `cmd:"" help:"Load generator with uplink StatObject"`
}
