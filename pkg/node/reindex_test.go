package node

import (
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"github.com/stretchr/testify/require"
	"storj.io/common/storj"
	"testing"
)

func TestPieceIDDerivation(t *testing.T) {
	rootPieceID, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString("5QELK4XENCTVET2KDRABOBTDE62ODBCQI3RWX3TWZMEXOGUNGY3A")
	require.NoError(t, err)
	p := storj.PieceID{}
	copy(p[:], rootPieceID)
	fmt.Println(p.String())
	nodeID, err := storj.NodeIDFromString("1JM5nNi3W8PvhfvsUPNiZMkiMhEA1QULuGS4Ap877VL67CyKwN")
	require.NoError(t, err)
	derived := p.Derive(nodeID, 19)
	fmt.Println(derived.String())
	fmt.Println(base64.StdEncoding.EncodeToString(derived.Bytes()))
	fmt.Println(base64.URLEncoding.EncodeToString(derived.Bytes()))
}
