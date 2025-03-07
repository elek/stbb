package hashstore

import "github.com/spacemonkeygo/monkit/v3"

var mon = monkit.Package()

type Hashstore struct {
	Convert Convert `cmd:"" usage:"import data to the hashtable"`
	List    List    `cmd:"" usage:"list content of a hashtable"`
	Stat    Stat    `cmd:"" usage:"list content of a hashtable stat"`
	//Generate Generate `cmd:"" usage:"generate data to a hashtable store"`
	Compact     Compact     `cmd:"" usage:"compact a hashtable store"`
	Report      Report      `cmd:"" usage:"show additional reports on a hashtable store"`
	Logs        Logs        `cmd:"" usage:"show current log file load"`
	TTLReport   TTLReport   `cmd:"" usage:"print out ttl expiration per file"`
	Recover     Recover     `cmd:"" usage:"recover hashtable (metadata) from a hashstore log files"`
	RestoreTime RestoreTime `cmd:"" usage:"get/set restore time for a satellite"`
	Get         Get         `cmd:"" usage:"get a record from a hashtable"`
	Diff        Diff        `cmd:"" usage:"diff two hashstore"`
}
