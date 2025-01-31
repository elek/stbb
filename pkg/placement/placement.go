package placement

type Placement struct {
	Select   Select   `cmd:"" help:"select given number of nodes from DB, matching the placement rule"`
	Nodes    Nodes    `cmd:"" help:"load the upload cache and print out statistics"`
	List     List     `cmd:"" help:"list nodes available for selection"`
	Tags     Tags     `cmd:"" help:"report current tag distribution"`
	Simulate Simulate `cmd:"" help:"selection simulation with histogram"`
}
