package crypto

type Crypto struct {
	DecryptKey  DecryptKey  `cmd:""`
	EncryptPath EncryptPath `cmd:"" usage:"encrypt path with access grant"`
	Crack       BruteForce  `cmd:""`
}
