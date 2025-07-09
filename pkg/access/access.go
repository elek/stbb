package access

type AccessCmd struct {
	Change Change `cmd:"" help:"change the encryption key in an access grant"`
	ApiKey ApiKey `cmd:"" help:"create api key from head and secret"`
}
