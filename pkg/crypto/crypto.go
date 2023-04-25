package crypto

type Crypto struct {
	DecryptKey DecryptKey `cmd:""`
	Crack      BruteForce `cmd:""`
}
