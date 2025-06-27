package segment

// Segment contains command which uses direct database connection.
type Segment struct {
	Histogram    Histogram    `cmd:"" help:"diversity histogram of a segment"`
	List         PieceList    `cmd:"" help:"list piece locations in a csv for one single segment"`
	Availability Availability `cmd:"" help:"test availability of the segment with calling exists endpoints"`
	Classify     Classify     `cmd:"" help:"execute piece classification on segment"`
	Download     Download     `cmd:"" help:"download all the available pieces ASAP"`
	Ecdecode     ECDecode     `cmd:"" help:"decode original segment from downloaded pieces"`
	Show         Show         `cmd:"" help:"show information about the segment"`
	Report       Report       `cmd:"" help:"show segments based on segment id list"`
	Nodes        Nodes        `cmd:"" help:"print details of nodes"`
	Checksum     Checksum     `cmd:"" help:"Check checksum of the downloaded segment"`
	Decrypt      Decrypt      `cmd:"" help:"Decrypt segment with the provided keys"`
}
