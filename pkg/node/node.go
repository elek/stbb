package node

type Node struct {
	Report      Report      `cmd:""`
	NodeStat    NodeStat    `cmd:""`
	Export      Export      `cmd:""`
	UnsentOrder UnsentOrder `cmd:""`
	Paystub     Paystub     `cmd:""`
	Checkin     Checkin     `cmd:""`
	PieceList   PieceList   `cmd:""`
	GeoIP       GeoIP       `cmd:""`
	Convert     Convert     `cmd:""`
	Usage       Usage       `cmd:"" help:"calls the info endpoint of the satellite in the name of the storagenode"`
}
