package load

type Load struct {
	Uplink        Uplink        `cmd:"" help:"load generator with uplink upload/download"`
	Stat          Stat          `cmd:"" help:"load generator with uplink StatObject"`
	PieceUpload   PieceUpload   `cmd:"" help:"execute upload with pieces store client"`
	PieceDownload PieceDownload `cmd:"" help:"execute download with pieces store client"`
}
