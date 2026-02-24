package nodeid

import (
	"crypto/x509"
	"path/filepath"

	"github.com/pkg/errors"
	"storj.io/common/identity"
)

type Sign struct {
	IdentityDir  string `arg:"" help:"Directory containing identity files"`
	AuthorityDir string `arg:"" help:"Directory containing identity files of the authority"`
}

func (s *Sign) Run() error {
	cfg := identity.Config{
		CertPath: filepath.Join(s.IdentityDir, "identity.cert"),
		KeyPath:  filepath.Join(s.IdentityDir, "identity.key"),
	}
	fi, err := cfg.Load()
	if err != nil {
		return errors.WithStack(err)
	}

	caConfig := identity.FullCAConfig{
		CertPath: filepath.Join(s.AuthorityDir, "ca.cert"),
		KeyPath:  filepath.Join(s.AuthorityDir, "ca.key"),
	}
	ca, err := caConfig.Load()
	if err != nil {
		return errors.WithStack(err)
	}

	signedPeerCA, err := ca.Sign(fi.CA)
	if err != nil {
		return errors.WithStack(err)
	}

	fi.RestChain = []*x509.Certificate{ca.Cert}
	fi.CA = signedPeerCA
	err = cfg.PeerConfig().Save(fi.PeerIdentity())
	if err != nil {
		return err
	}

	return nil
}
