package downloadng

import (
	"encoding/base64"
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"storj.io/storj/cmd/uplink/ulloc"
)

var DownloadCmd = &cobra.Command{
	Use: "downloadng",
}

func init() {
	stbb.RootCmd.AddCommand(DownloadCmd)
	DownloadCmd.RunE = func(cmd *cobra.Command, args []string) error {
		p, err := ulloc.Parse(args[0])
		if err != nil {
			return err
		}
		bucket, key, ok := p.RemoteParts()

		decoded, err := base64.URLEncoding.DecodeString(key)
		if err != nil {
			return err
		}

		if !ok {
			return errs.New("Path is not remote %s", args[0])
		}

		return download([]byte(bucket), decoded)
	}
}
