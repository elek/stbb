package satellite

type Satellite struct {
	Run     Run     `cmd:"" help:"Run mock satellite"`
	Restore Restore `cmd:"" help:"Send restore trash request to the storagenode (satellite->sn)"`
	Ping    Ping    `cmd:"" help:"Send ping to the storagenode (satellite->sn)"`
	Time    Time    `cmd:"" help:"Get time of the satellite (sn->satellite)"`
	GC      GC      `cmd:"" help:"Send gc request to the storagenode"`
}
