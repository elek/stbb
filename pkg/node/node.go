package node

type Node struct {
	Scan     Scan     `cmd:""`
	Report   Report   `cmd:""`
	NodeStat NodeStat `cmd:""`
}
