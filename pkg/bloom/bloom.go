package bloom

type Bloom struct {
	Create CreateFilter `cmd:"" help:"create bloom filter based on a list of piece IDs"`
	Check  Check        `cmd:"" help:"checks piece ids (from file) against a bloom filter"`
	Send   Send         `cmd:"" help:"send bloom filter to a storagenode"`
}
