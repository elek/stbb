package node

type Node struct {
	Report Report `cmd:""`

	PricingModel PricingModel `cmd:"" help:"Get pricing model from satellite (sn->satellite RPC)"`
	Checkin      Checkin      `cmd:"" help:"node checking rpc call (sn->satellite RPC)"`
	Usage        Usage        `cmd:"" help:"calls the info endpoint of the satellite in the name of the storagenode (sn->satellite RPC)"`

	Enrich Enrich `cmd:"" help:"enrich node information with metadata from the satellite"`

	Export         Export         `cmd:""`
	UnsentOrder    UnsentOrder    `cmd:"" help:"parse unsent order archive"`
	Paystub        Paystub        `cmd:""`
	PieceList      PieceList      `cmd:"" help:"generate list of pieces for one node"`
	PieceListCheck PieceListCheck `cmd:"" help:"check the generated list based on a real blobstore"`
	GeoIP          GeoIP          `cmd:"" help:"opens and prints out maxmind GeoIP database"`
	Convert        Convert        `cmd:""`
	Info           Info           `cmd:"" help:"information for one node"`
}
