package downloadng

import (
	"github.com/zeebo/errs"
	"runtime"
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
func readStack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
