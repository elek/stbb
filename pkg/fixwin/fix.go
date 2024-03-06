package main

import (
	"bufio"
	"encoding/base32"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"log"
	"os"
	"storj.io/common/storj"
	"strings"
)

var pathEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func main() {
	err := run()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}

func run() error {
	input, err := os.Open(os.Args[1])
	if err != nil {
		return errors.WithStack(err)
	}
	// Make an tranformer that converts MS-Win default to UTF8:
	win16be := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM)
	// Make a transformer that is like win16be, but abides by BOM:
	utf16bom := unicode.BOMOverride(win16be.NewDecoder())

	// Make a Reader that uses utf16bom:
	unicodeReader := transform.NewReader(input, utf16bom)

	reader := bufio.NewReader(unicodeReader)
	var line []byte
	var id storj.PieceID
	ix := 0
	prefix := ""
	for {
		line, _, err = reader.ReadLine()
		if err != nil {
			return err
		}
		strLine := strings.TrimSpace(string(line))
		if strLine == "" {
			continue
		}
		if strings.HasPrefix(strLine, "Directory") {
			parts := strings.Split(strLine, "\\")
			prefix = parts[len(parts)-1]
			continue
		}
		if !strings.HasSuffix(strLine, ".sj1") {
			continue
		}
		if prefix == "" {
			panic("no prefix")
		}
		parts := strings.Split(strLine, " ")
		//fmt.Println(strings.TrimSuffix(parts[len(parts)-1], ".sj1") + "," + parts[len(parts)-2])
		raw, err := pathEncoding.DecodeString(strings.TrimSuffix(prefix+parts[len(parts)-1], ".sj1"))
		if err != nil {
			return err
		}
		copy(id[:], raw)
		fmt.Println(id.String())
		ix++

	}
	return nil

}
