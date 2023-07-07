package downloadng

import (
	"github.com/zeebo/errs"
	"storj.io/storj/cmd/uplink/ulloc"
)

type DownloadCmd struct {
	Path string `arg:""`
}

func (d DownloadCmd) Run() error {

	p, err := ulloc.Parse(d.Path)
	if err != nil {
		return err
	}
	bucket, key, ok := p.RemoteParts()

	if !ok {
		return errs.New("Path is not remote %s", d.Path)
	}

	return download(bucket, key)

}
