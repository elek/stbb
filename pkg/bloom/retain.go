package bloom

type Retain struct {
	Hashstore   string `arg:"" help:"the path to the hashstore"`
	BloomFilter string `arg:"" help:"the path to the bloom filter"`
}

func (r *Retain) Run() error {
	return nil
}
