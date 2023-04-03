package stbb

import (
	"encoding/hex"
	"fmt"
	"github.com/zeebo/errs/v2"
	"storj.io/common/paths"
	"strings"
)

var (
	emptyComponentPrefix    = byte('\x01')
	notEmptyComponentPrefix = byte('\x02')

	escapeSlash = byte('\x2e')
	escapeFF    = byte('\xfe')
	escape01    = byte('\x01')
)

type Path struct {
	Decrypt Decrypt `cmd:""`
}

type Decrypt struct {
	Encrypted string `arg:""`
}

func (d Decrypt) Run() error {
	decodeString, err := hex.DecodeString(d.Encrypted)
	if err != nil {
		return errs.Wrap(err)
	}
	iterator := paths.NewIterator(string(decodeString))
	res := []string{}
	for !iterator.Done() {
		pathSegment, err := decodeSegment([]byte(iterator.Next()))
		if err != nil {
			return errs.Wrap(err)
		}
		res = append(res, hex.EncodeToString(pathSegment[24:]))
	}
	fmt.Println("/" + strings.Join(res, "/"))
	return nil
}

func decodeSegment(segment []byte) ([]byte, error) {
	err := validateEncodedSegment(segment)
	if err != nil {
		return []byte{}, err
	}
	if segment[0] == emptyComponentPrefix {
		return []byte{}, nil
	}

	currentIndex := 0
	for i := 1; i < len(segment); i++ {
		switch {
		case i == len(segment)-1:
			segment[currentIndex] = segment[i]
		case segment[i] == escapeSlash || segment[i] == escapeFF:
			segment[currentIndex] = segment[i] + segment[i+1] - 1
			i++
		case segment[i] == escape01:
			segment[currentIndex] = segment[i+1] - 1
			i++
		default:
			segment[currentIndex] = segment[i]
		}
		currentIndex++
	}
	return segment[:currentIndex], nil
}

// validateEncodedSegment checks if:
// * The last byte/sequence is not in {escape1, escape2, escape3}.
// * Any byte after an escape character is \x01 or \x02.
// * It does not contain any characters in {\x00, \xff, \x2f}.
// * It is non-empty.
// * It begins with a character in {\x01, \x02}.
func validateEncodedSegment(segment []byte) error {
	switch {
	case len(segment) == 0:
		return errs.Errorf("encoded segment cannot be empty")
	case segment[0] != emptyComponentPrefix && segment[0] != notEmptyComponentPrefix:
		return errs.Errorf("invalid segment prefix")
	case segment[0] == emptyComponentPrefix && len(segment) > 1:
		return errs.Errorf("segment encoded as empty but contains data")
	case segment[0] == notEmptyComponentPrefix && len(segment) == 1:
		return errs.Errorf("segment encoded as not empty but doesn't contain data")
	}

	if len(segment) == 1 {
		return nil
	}

	index := 1
	for ; index < len(segment)-1; index++ {
		if isEscapeByte(segment[index]) {
			if segment[index+1] == 1 || segment[index+1] == 2 {
				index++
				continue
			}
			return errs.Errorf("invalid escape sequence")
		}
		if isDisallowedByte(segment[index]) {
			return errs.Errorf("invalid character in segment")
		}
	}
	if index == len(segment)-1 {
		if isEscapeByte(segment[index]) {
			return errs.Errorf("invalid escape sequence")
		}
		if isDisallowedByte(segment[index]) {
			return errs.Errorf("invalid character")
		}
	}

	return nil
}

func isEscapeByte(b byte) bool {
	return b == escapeSlash || b == escapeFF || b == escape01
}

func isDisallowedByte(b byte) bool {
	return b == 0 || b == '\xff' || b == '/'
}
