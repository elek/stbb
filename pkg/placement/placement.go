package placement

type Placement struct {
	Select     Select     `cmd:"" help:"select given number of nodes from DB, matching the placement rule"`
	SelectPool SelectPool `cmd:"" help:"select given number of nodes from DB, printing simplified pool stat"`
	Nodes      Nodes      `cmd:"" help:"load the upload cache and print out statistics"`
	List       List       `cmd:"" help:"list nodes available for selection"`
	Tags       Tags       `cmd:"" help:"report current tag distribution"`
	QueryTags  QueryTags  `cmd:"" help:"generate query for tags"`
	Simulate   Simulate   `cmd:"" help:"selection simulation with histogram"`
	Score      Score      `cmd:"" help:"print out node scores"`
}
