package hashstore

import "github.com/spacemonkeygo/monkit/v3"

var mon = monkit.Package()

type Hashstore struct {
	Convert Convert `cmd:"" usage:"import data to the hashtable"`
	List    List    `cmd:"" usage:"list content of a hastable"`
}