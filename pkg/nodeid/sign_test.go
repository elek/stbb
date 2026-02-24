package nodeid

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	origCerts := readCerts(t, "testcerts/nodeid/identity.cert")
	require.Len(t, origCerts, 2, "original identity.cert should have 2 certificates")

	for _, tc := range []struct {
		name string
		ca   string
	}{
		{"ca1", "testcerts/ca1"},
		{"ca2", "testcerts/ca2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// copy testcerts/nodeid to a temp dir so we don't modify the originals
			tmpDir := t.TempDir()
			nodeDir := filepath.Join(tmpDir, "nodeid")
			require.NoError(t, os.MkdirAll(nodeDir, 0755))
			for _, f := range []string{"identity.cert", "identity.key", "ca.cert", "ca.key"} {
				data, err := os.ReadFile(filepath.Join("testcerts/nodeid", f))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(nodeDir, f), data, 0644))
			}

			s := Sign{
				IdentityDir:  nodeDir,
				AuthorityDir: tc.ca,
			}
			require.NoError(t, s.Run())

			// After signing, identity.cert should contain 3 certificates:
			//   cert1 = leaf (node identity), signed by cert2
			//   cert2 = node CA, signed by cert3
			//   cert3 = authority CA (self-signed)
			certs := readCerts(t, filepath.Join(nodeDir, "identity.cert"))
			require.Len(t, certs, 3, "signed identity.cert should have 3 certificates")

			// In the file: [leaf, node CA signed by authority, authority CA]
			// Logical chain: cert1 (leaf) -> cert2 (node CA) -> cert3 (authority)
			cert1 := certs[0] // leaf (node identity)
			cert2 := certs[1] // node CA signed by authority
			cert3 := certs[2] // authority CA (self-signed)

			// cert1 (leaf) is signed by cert2 (node CA)
			require.NoError(t, cert1.CheckSignatureFrom(cert2),
				"cert1 (leaf) should be signed by cert2 (node CA)")

			// cert2 (node CA) is signed by cert3 (authority CA)
			require.NoError(t, cert2.CheckSignatureFrom(cert3),
				"cert2 (node CA) should be signed by cert3 (authority CA)")

			// cert3 (authority CA) is self-signed
			require.NoError(t, cert3.CheckSignatureFrom(cert3),
				"cert3 (authority CA) should be self-signed")

			// cert1 public key should match original leaf
			require.True(t, pubKeysEqual(origCerts[0].PublicKey, cert1.PublicKey),
				"cert1 public key should match original leaf")

			// cert2 public key should match original CA (the node's CA)
			require.True(t, pubKeysEqual(origCerts[1].PublicKey, cert2.PublicKey),
				"cert2 public key should match original node CA")

			// cert3 public key should match the authority CA
			authorityCerts := readCerts(t, filepath.Join(tc.ca, "ca.cert"))
			require.True(t, pubKeysEqual(authorityCerts[0].PublicKey, cert3.PublicKey),
				"cert3 public key should match authority CA")
		})
	}
}

func TestSignTwice(t *testing.T) {
	tmpDir := t.TempDir()
	nodeDir := filepath.Join(tmpDir, "nodeid")
	require.NoError(t, os.MkdirAll(nodeDir, 0755))
	for _, f := range []string{"identity.cert", "identity.key", "ca.cert", "ca.key"} {
		data, err := os.ReadFile(filepath.Join("testcerts/nodeid", f))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(nodeDir, f), data, 0644))
	}

	// Sign with ca2 twice — the second run must be able to load the identity
	// produced by the first run without "x509: ECDSA verification failure".
	for i := 0; i < 2; i++ {
		s := Sign{
			IdentityDir:  nodeDir,
			AuthorityDir: "testcerts/ca2",
		}
		require.NoError(t, s.Run(), "sign run %d failed", i+1)
	}
}

func readCerts(t *testing.T, path string) []*x509.Certificate {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var certs []*x509.Certificate
	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		certs = append(certs, cert)
	}
	return certs
}

func pubKeysEqual(a, b interface{}) bool {
	ak, aOk := a.(*ecdsa.PublicKey)
	bk, bOk := b.(*ecdsa.PublicKey)
	if !aOk || !bOk {
		return false
	}
	return ak.X.Cmp(bk.X) == 0 && ak.Y.Cmp(bk.Y) == 0
}
