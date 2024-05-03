package bloom

type Bloom struct {
	Create     CreateFilter `cmd:"" help:"create bloom filter based on a list of piece IDs"`
	Generate   Generate     `cmd:"" help:"generate bloom filter with all one"`
	Check      Check        `cmd:"" help:"checks piece ids (from file) against a bloom filter"`
	Info       Info         `cmd:"" help:"print out bloom filter metadata"`
	Send       Send         `cmd:"" help:"send bloom filter to a storagenode, with raw RPC call"`
	SendClient SendClient   `cmd:"" help:"send bloom filter to a storagenode, with piecestore client"`
	Find       Find         `cmd:"" help:"Find BF for specific nodes in the generated ZIP files"`
}
