package nodeid

import (
	"context"
	"crypto"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"storj.io/common/base58"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/peertls"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/pkcrypto"
	"storj.io/common/rpc"
	"storj.io/common/rpc/noise"
	"storj.io/common/storj"
	"storj.io/drpc"
	"strings"
	"time"
)

type NodeID struct {
	Decode   Decode   `cmd:"" help:"Decode base64 nodeid to binary"`
	Read     Read     `cmd:""`
	Encode   Encode   `cmd:"" help:"encode raw nodeid to base64"`
	Remote   Remote   `cmd:"" help:"read nodeid from remote DRPC port"`
	Generate Generate `cmd:""`
	Noise    Noise    `cmd:""`
}

type Decode struct {
	ID string `arg:""`
}

func (d *Decode) Run() error {
	id, err := storj.NodeIDFromString(d.ID)
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(id.Bytes()))
	return nil
}

type Read struct {
	ID string `arg:""`
}

func (r *Read) Run() error {
	id, err := identity.NodeIDFromCertPath(r.ID)
	if err != nil {
		return err
	}
	fmt.Println(id.String())
	return nil
}

type Noise struct {
	Path string `arg:"" default:"."`
}

func (n *Noise) Run() error {
	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(n.Path, "identity.cert"),
		KeyPath:  filepath.Join(n.Path, "identity.key"),
	}
	id, err := satelliteIdentityCfg.Load()
	if err != nil {
		return err
	}
	attestation, err := noise.GenerateKeyAttestation(context.Background(), id, &pb.NoiseInfo{})
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(attestation.NoisePublicKey))
	fmt.Println(base58.CheckEncode([]byte(attestation.NoisePublicKey), 0))
	return nil

}

type Encode struct {
	ID string `arg:""`
}

func (e *Encode) Run() error {
	bs, err := hex.DecodeString(e.ID)
	if err != nil {
		return err
	}

	id, err := storj.NodeIDFromBytes(bs)
	if err != nil {
		return err
	}
	fmt.Println(id.String())
	return nil
}

type Remote struct {
	HostPort string `arg:""`
}

func (r *Remote) Run() error {
	ctx := context.Background()
	id, err := GetSatelliteID(ctx, r.HostPort)
	if err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}

type Generate struct {
}

func (g *Generate) Run() error {
	for {
		version := storj.IDVersions[storj.V0]
		k, err := version.NewPrivateKey()
		if err != nil {
			return err
		}

		var pubKey crypto.PublicKey
		pubKey, err = pkcrypto.PublicKeyFromPrivate(k)
		if err != nil {
			return err
		}
		nodeID, err := identity.NodeIDFromKey(pubKey, version)
		if err != nil {
			return err
		}
		if strings.HasSuffix(nodeID.String(), "prdn") {
			fmt.Println(nodeID.String())
			err = pkcrypto.WritePrivateKeyPEM(os.Stdout, k)
			if err != nil {
				return err
			}

			ct, err := peertls.CATemplate()
			if err != nil {
				return err
			}
			cert, err := peertls.CreateSelfSignedCertificate(k, ct)
			if err != nil {
				return err
			}
			ca := &identity.FullCertificateAuthority{
				Cert: cert,
				Key:  k,
				ID:   nodeID,
			}
			fmt.Println(ca)
			break
		}
	}
	return nil
}

// GetSatelliteID retrieves node identity from SSL endpoint.
// Only for testing. Using identified node id is not reliable.
func GetSatelliteID(ctx context.Context, address string) (string, error) {
	tlsOptions, err := getProcessTLSOptions(ctx)
	if err != nil {
		return "", err
	}

	dialer := rpc.NewDefaultDialer(tlsOptions)
	dialer.Pool = rpc.NewDefaultConnectionPool()

	dialer.DialTimeout = 10 * time.Second
	// TODO
	//dialContext := socket.BackgroundDialer().DialContext
	//
	////lint:ignore SA1019 it's safe to use TCP here instead of QUIC + TCP
	//dialer.Connector = rpc.NewDefaultTCPConnector(&rpc.ConnectorAdapter{DialContext: dialContext}) //nolint:staticcheck

	conn, err := dialer.DialAddressInsecure(ctx, address)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	in := struct{}{}
	out := struct{}{}
	_ = conn.Invoke(ctx, "asd", &NullEncoding{}, in, out)
	peerIdentity, err := conn.PeerIdentity()
	if err != nil {
		return "", err
	}

	return peerIdentity.ID.String() + "@" + address, nil

}

func getProcessTLSOptions(ctx context.Context) (*tlsopts.Options, error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return nil, err
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return nil, err
	}

	return tlsOptions, nil
}

type NullEncoding struct {
}

func (n NullEncoding) Marshal(msg drpc.Message) ([]byte, error) {
	return []byte{1}, nil
}

func (n NullEncoding) Unmarshal(buf []byte, msg drpc.Message) error {
	return nil
}

var _ drpc.Encoding = &NullEncoding{}
