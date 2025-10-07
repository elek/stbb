package admin

type Admin struct {
	SetBucketPlacement SetBucketPlacement `cmd:"" help:"set the default placement of a bucket"`
	UpdateUser         UpdateUser         `cmd:"" help:"update some fields of a user"`
}
