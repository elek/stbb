package metainfo

type Metainfo struct {
	ProjectInfo    ProjectInfo    `cmd:""`
	DownloadObject DownloadObject `cmd:"" help:"Call the DownloadObject metainfo endpoint"`
	BeginObject    BeginObject    `cmd:"" help:"Call the BeginObject metainfo endpoint"`
}
