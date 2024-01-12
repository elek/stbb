package segment

// Segment contains command which uses direct database connection.
type Segment struct {
	List         PieceList    `cmd:""`
	Availability Availability `cmd:""`
	Classify     Classify     `cmd:""`
}
