package nodeid

import (
	"crypto/x509"
	"github.com/pkg/errors"
	"path/filepath"
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
	//signedChainBytes := [][]byte{signedPeerCA.Raw, ca.Cert.Raw}
	//signedChainBytes = append(signedChainBytes, ca.RawRestChain()...)
	//
	//signedChain, err := pkcrypto.CertsFromDER(signedChainBytes)
	//if err != nil {
	//	return nil
	//}
	//for _, s := range signedChain {
	//	fmt.Println(s.PublicKey)
	//}
	//
	fi.RestChain = []*x509.Certificate{signedPeerCA}
	fi.CA = ca.Cert
	err = cfg.PeerConfig().Save(fi.PeerIdentity())
	if err != nil {
		return err
	}

	return nil
}
