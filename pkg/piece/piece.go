package piece

import (
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/spf13/cobra"
)

var mon = monkit.Package()

var PieceCmd = &cobra.Command{
	Use: "piece",
}

type Piece struct {
	UploadDrpc   UploadDrpc         `cmd:"" help:"Upload piece to the Storagenode"`
	DownloadDrpc DownloadDRPC       `cmd:"" help:"Download piece from the Storagenode"`
	List         List               `cmd:"" help:"Print out pieces for one particular object"`
	Nodes        Nodes              `cmd:"" help:"Print out piece locations with pieceID and node ID"`
	NodeSpeed    NodeSpeed          `cmd:"" help:"Download one piece from all the nodes"`
	Decode       Decode             `cmd:"" help:"Decode piece alias (from the condensed format, stored in db)"`
	DownloadPs   DownloadPieceStore `cmd:"" help:"Download piece from the Storagenode using piece store"`
	Unalias      Unalias            `cmd:"" help:"Decode node aliases"`
	Exist        Exist              `cmd:"" help:"check if piece id is on SN"`
}
