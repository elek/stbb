package dir

type Dir struct {
	OpenDir OpenDir `cmd:""`
	ReadDir ReadDir `cmd:""`
	Walk    Walk    `cmd:""`
}
