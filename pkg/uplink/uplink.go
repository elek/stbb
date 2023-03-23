package uplink

type Uplink struct {
	Upload   Upload   `cmd:"" help:"Upload a file with uplink"`
	Download Download `cmd:"" help:"Download a file with uplink"`
	UpDown   UpDown   `cmd:"" help:"Upload & Download a file with uplink"`
}
