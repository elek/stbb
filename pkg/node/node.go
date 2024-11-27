package node

type Node struct {
	Report         Report         `cmd:""`
	NodeStat       NodeStat       `cmd:"nodestat call (sn->satellite)"`
	Export         Export         `cmd:""`
	UnsentOrder    UnsentOrder    `cmd:"" usage:"parse unsent order archive"`
	Paystub        Paystub        `cmd:""`
	Checkin        Checkin        `cmd:"" help:"node checking rpc call (sn->satellite)"`
	PieceList      PieceList      `cmd:"" help:"generate list of pieces for one node"`
	PieceListCheck PieceListCheck `cmd:"" help:"check the generated list based on a real blobstore"`
	GeoIP          GeoIP          `cmd:"" help:"opens and prints out maxmind GeoIP database"`
	Convert        Convert        `cmd:""`
	Usage          Usage          `cmd:"" help:"calls the info endpoint of the satellite in the name of the storagenode (sn->satellite)"`
	Info           Info           `cmd:"" help:"information for one node"`
}
