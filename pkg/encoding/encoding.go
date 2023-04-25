package encoding

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"storj.io/common/base58"
)

type Encoding struct {
	Base58Decode Base58Decode `cmd:"" name:"base58-decode"`
	Base58Encode Base58Encode `cmd:"" name:"base58-encode"`
	Base32Decode Base32Decode `cmd:"" name:"base32-decode"`
	Base32Encode Base32Encode `cmd:"" name:"base32-encode"`
	Base64Decode Base64Decode `cmd:"" name:"base64-decode"`
	Base64Encode Base64Encode `cmd:"" name:"base64-encode"`
	HexEncode    HexEncode    `cmd:"" name:"hex-encode"`
	HexDecode    HexDecode    `cmd:"" name:"hex-decode"`
}

type Base58Decode struct {
	Data string `arg:""`
}

func (b Base58Decode) Run() error {
	result, _, err := base58.CheckDecode(b.Data)
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(result))
	return nil
}

type Base58Encode struct {
	Data string `arg:""`
}

func (b Base58Encode) Run() error {
	parsed, err := hex.DecodeString(b.Data)
	s := base58.CheckEncode(parsed, 0)
	if err != nil {
		return err
	}
	fmt.Println(s)
	return nil
}

type Base32Decode struct {
	Data string `arg:""`
}

func (b Base32Decode) Run() error {
	result, err := base32.StdEncoding.DecodeString(b.Data)
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(result))
	return nil
}

type Base32Encode struct {
	Data string `arg:""`
}

func (b Base32Encode) Run() error {
	raw, err := hex.DecodeString(b.Data)
	if err != nil {
		return err
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
	if err != nil {
		return err
	}
	fmt.Println(encoded)
	return nil
}

type Base64Decode struct {
	Data string `arg:""`
}

func (b Base64Decode) Run() error {
	result, err := base64.URLEncoding.DecodeString(b.Data)
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(result))
	return nil
}

type Base64Encode struct {
	Data string `arg:""`
}

func (b Base64Encode) Run() error {
	raw, err := hex.DecodeString(b.Data)
	if err != nil {
		return err
	}
	result := base64.URLEncoding.EncodeToString(raw)

	fmt.Println(result)
	return nil
}

type HexEncode struct {
	Data string `arg:""`
}

func (b HexEncode) Run() error {
	result := hex.EncodeToString([]byte(b.Data))
	fmt.Println(result)
	return nil
}

type HexDecode struct {
	Data string `arg:""`
}

func (b HexDecode) Run() error {
	result, err := hex.DecodeString(b.Data)
	if err != nil {
		return err
	}
	fmt.Println(result)
	fmt.Println(string(result))
	return nil
}
