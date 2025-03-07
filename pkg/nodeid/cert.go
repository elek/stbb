package nodeid

import (
	"crypto"
	"crypto/ecdsa"
	"hash"
	"strconv"

	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"github.com/pkg/errors"
	"math/big"
	"os"
	"path/filepath"
)

type Cert struct {
	Files []string `arg:""  help:"certificate or key files to be read"`
}

func (c *Cert) Run() error {
	allKeys := make(map[string]*ecdsa.PublicKey)
	externalKeys := make(map[string]*ecdsa.PublicKey)
	for _, f := range c.Files {
		if filepath.Ext(f) != ".key" {
			continue
		}
		keyBytes, err := os.ReadFile(f)
		if err != nil {
			return errors.WithStack(err)
		}
		block, _ := pem.Decode(keyBytes)
		privateKey, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return errors.WithStack(err)
		}
		allKeys[filepath.Base(f)] = &privateKey.PublicKey
		fmt.Println(filepath.Base(f), privateKey.PublicKey.X.Text(16), privateKey.PublicKey.Y.Text(16))
		externalKeys[filepath.Base(f)] = &privateKey.PublicKey
	}

	for _, f := range c.Files {
		if filepath.Ext(f) != ".cert" {
			continue
		}
		certBytes, err := os.ReadFile(f)
		if err != nil {
			return errors.WithStack(err)
		}

		var certs []*x509.Certificate
		for block, rest := pem.Decode(certBytes); block != nil; block, rest = pem.Decode(rest) {
			if block.Type != "CERTIFICATE" {
				continue
			}

			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return errors.WithStack(err)
			}

			certs = append(certs, cert)
		}

		for i, cert := range certs {
			pubKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
			if !ok {
				continue
			}
			allKeys["cert"+strconv.Itoa(i+1)] = pubKey
		}

		for i, cert := range certs {
			fmt.Printf("\ncert%d:\n", i+1)
			fmt.Printf("Subject: %s\n", cert.Subject)
			fmt.Printf("Issuer: %s\n", cert.Issuer)
			fmt.Printf("Valid from: %s\n", cert.NotBefore)
			fmt.Printf("Valid until: %s\n", cert.NotAfter)
			fmt.Printf("Serial number: %d\n", cert.SerialNumber)
			fmt.Printf("SignatureAlgo: %d\n", cert.SignatureAlgorithm)

			pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
			if !ok {
				continue
			}
			fmt.Printf("Public key: ECDSA (curve: %s) %s %s\n", pub.Curve.Params().Name, pub.X.Text(16), pub.Y.Text(16))
			for name, key := range externalKeys {
				if pub.X.Cmp(key.X) == 0 && pub.Y.Cmp(key.Y) == 0 {
					fmt.Println("Public key of:", name)
				}
			}
			for name, key := range allKeys {
				ok, err := verifySignature(cert, key)
				if err != nil {
					return errors.WithStack(err)
				}
				if ok {
					fmt.Println("signed with", name)
				}
			}

		}

	}

	return nil
}

func verifySignature(cert *x509.Certificate, pub *ecdsa.PublicKey) (bool, error) {
	var ecdsaSig ECDSASignature
	_, err := asn1.Unmarshal(cert.Signature, &ecdsaSig)
	if err != nil {
		return false, errors.WithStack(err)
	}
	hasher, err := getHasher(cert.SignatureAlgorithm)
	if err != nil {
		return false, errors.WithStack(err)
	}
	hasher.Write(cert.RawTBSCertificate)
	verify := ecdsa.Verify(pub, hasher.Sum([]byte{}), ecdsaSig.R, ecdsaSig.S)
	return verify, nil
}

type ECDSASignature struct {
	R, S *big.Int
}

func getHasher(algo x509.SignatureAlgorithm) (hash.Hash, error) {
	var hashType crypto.Hash
	switch algo {
	case x509.ECDSAWithSHA1:
		hashType = crypto.SHA1
	case x509.ECDSAWithSHA256:
		hashType = crypto.SHA256
	case x509.ECDSAWithSHA384:
		hashType = crypto.SHA384
	case x509.ECDSAWithSHA512:
		hashType = crypto.SHA512
	default:
		return nil, fmt.Errorf("unsupported signature algorithm: %v", algo)
	}
	return hashType.New(), nil
}
