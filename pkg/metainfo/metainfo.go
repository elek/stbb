package metainfo

type Metainfo struct {
	ProjectInfo    ProjectInfo    `cmd:""`
	DownloadObject DownloadObject `cmd:"" help:"Call the DownloadObject metainfo endpoint"`
}
