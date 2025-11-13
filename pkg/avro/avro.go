package avro

type Avro struct {
	Find Find `cmd:"" help:"Find and print record in remote avro file."`
	List List `cmd:"" help:"List all avro files and the first id in the file."`
}
