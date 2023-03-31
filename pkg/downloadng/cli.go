package downloadng

import (
	"encoding/base64"
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

	decoded, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return err
	}

	if !ok {
		return errs.New("Path is not remote %s", d.Path)
	}

	return download([]byte(bucket), decoded)

}
