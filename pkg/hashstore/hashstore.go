package hashstore

import "github.com/spacemonkeygo/monkit/v3"

var mon = monkit.Package()

type Hashstore struct {
	Convert Convert `cmd:"" help:"import data to the hashtable"`
	List    List    `cmd:"" help:"list content of a hashtable"`
	Stat    Stat    `cmd:"" help:"list content of a hashtable stat"`
	//Generate Generate `cmd:"" help:"generate data to a hashtable store"`
	Compact     Compact     `cmd:"" help:"compact a hashtable store"`
	Report      Report      `cmd:"" help:"show additional reports on a hashtable store"`
	Logs        Logs        `cmd:"" help:"show current log file load"`
	TTLReport   TTLReport   `cmd:"" help:"print out ttl expiration per file"`
	Recover     Recover     `cmd:"" help:"recover hashtable (metadata) from a hashstore log files"`
	RestoreTime RestoreTime `cmd:"" help:"get/set restore time for a satellite"`
	Get         Get         `cmd:"" help:"get a record from a hashtable"`
	Diff        Diff        `cmd:"" help:"diff two hashstore"`
	Audit       Audit       `cmd:"" help:"audit a hashstore: check if pieces are included"`
	LogRead     LogRead     `cmd:"" help:"find record in hashstore log files without using metadata"`
}
