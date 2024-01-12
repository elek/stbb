package node

type Node struct {
	Scan        Scan        `cmd:""`
	Report      Report      `cmd:""`
	NodeStat    NodeStat    `cmd:""`
	Export      Export      `cmd:""`
	UnsentOrder UnsentOrder `cmd:""`
	Paystub     Paystub     `cmd:""`
	Checkin     Checkin     `cmd:""`
	PieceList   PieceList   `cmd:""`
}
