package piece

import (
	"context"
	"os"
	"path/filepath"
	"storj.io/common/grant"
	"storj.io/common/identity"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
)

type Downloader struct {
	satelliteURL   storj.NodeURL
	storagenodeURL storj.NodeURL
	fi             *identity.FullIdentity
	signee         signing.Signer
	dialer         rpc.Dialer
	grant          *grant.Access
}

func NewDownloader(ctx context.Context, storagenodeURL string) (d Downloader, err error) {
	gr := os.Getenv("UPLINK_ACCESS")
	d.grant, err = grant.ParseAccess(gr)
	if err != nil {
		return d, err
	}
	d.satelliteURL, err = storj.ParseNodeURL(d.grant.SatelliteAddress)
	if err != nil {
		return
	}

	d.storagenodeURL, err = storj.ParseNodeURL(storagenodeURL)
	if err != nil {
		return
	}

	d.dialer, err = getDialer(ctx)
	if err != nil {
		return
	}

	keysDir := os.Getenv("STBB_KEYS")
	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(keysDir, "identity.cert"),
		KeyPath:  filepath.Join(keysDir, "identity.key"),
	}
	d.fi, err = satelliteIdentityCfg.Load()
	if err != nil {
		return
	}

	d.signee = signing.SignerFromFullIdentity(d.fi)
	return
}
