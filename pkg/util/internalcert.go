package util

import (
	_ "embed"
)

var (
	//go:embed identity.cert
	Certificate []byte

	//go:embed identity.key
	Key []byte
)
