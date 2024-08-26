package node

type Node struct {
	Report         Report         `cmd:""`
	NodeStat       NodeStat       `cmd:""`
	Export         Export         `cmd:""`
	UnsentOrder    UnsentOrder    `cmd:""`
	Paystub        Paystub        `cmd:""`
	Checkin        Checkin        `cmd:""`
	PieceList      PieceList      `cmd:"" help:"generate list of pieces for one node"`
	PieceListCheck PieceListCheck `cmd:"" help:"check the generated list based on a real blobstore"`
	GeoIP          GeoIP          `cmd:""`
	Convert        Convert        `cmd:""`
	Usage          Usage          `cmd:"" help:"calls the info endpoint of the satellite in the name of the storagenode (sn->satellite)"`
}
