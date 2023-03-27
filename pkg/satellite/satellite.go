package satellite

type Satellite struct {
	Run     Run     `cmd:"" help:"Run mock satellite"`
	Restore Restore `cmd:"" help:"Send restore trash request to the storagenode"`
	Ping    Ping    `cmd:"" help:"Send ping to the storagenode"`
	GC      GC      `cmd:"" help:"Send gc request to the storagenode"`
}
