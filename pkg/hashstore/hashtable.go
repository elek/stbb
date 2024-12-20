package hashstore

import "github.com/spacemonkeygo/monkit/v3"

var mon = monkit.Package()

type Hashstore struct {
	Convert  Convert  `cmd:"" usage:"import data to the hashtable"`
	List     List     `cmd:"" usage:"list content of a hashtable"`
	Stat     Stat     `cmd:"" usage:"list content of a hashtable stat"`
	Generate Generate `cmd:"" usage:"generate data to a hashtable store"`
	Compact  Compact  `cmd:"" usage:"compact a hashtable store"`
	Report   Report   `cmd:"" usage:"show additional reports on a hashtable store"`
}
