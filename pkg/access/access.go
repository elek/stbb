package access

type AccessCmd struct {
	Host Host `cmd:"" help:"change the host name in an access grant"`
	Key  Key  `cmd:"" help:"change the encryption key in an access grant"`
}
