package hashstore

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"strings"
)

type WithHashstore struct {
	LogDir  string `help:"directory of the store" `
	MetaDir string `help:"directory of the hashtable files"`
	Path    string `arg:"" help:"the path to the hashtable file (or directory with one hashtbl file)" optional:"true"`
	Logs    string `arg:""  optional:"true"`
}

func (w WithHashstore) GetPath() (string, string) {
	metaPath := ""
	logPath := ""

	var err error

	if strings.HasPrefix(w.Path, "@") {
		id, tbl, ok := strings.Cut(strings.TrimPrefix(w.Path, "@"), "/")
		if !ok {
			panic("Use the format @storagenode1234/s0")
		}

		metaPath, err = pickFirstTbl(fmt.Sprintf("/opt/snmeta/%s/hashstore/12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S/%s/meta", id, tbl))
		if err != nil {
			metaPath, err = pickFirstTbl(fmt.Sprintf("/opt/%s/config/storage/hashstore/12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S/%s/meta", id, tbl))
			if err != nil {
				panic("Couldn't find log directory: " + err.Error())
			}
		}

		logPath = fmt.Sprintf("/opt/%s/config/storage/hashstore/12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S/%s/", id, tbl)
		return metaPath, logPath
	}

	if w.Path != "" {
		metaPath = w.Path
	}
	if w.Logs != "" {
		logPath = w.Logs
	}
	if w.MetaDir != "" {
		metaPath = w.MetaDir
	}
	if w.LogDir != "" {
		logPath = w.LogDir
	}
	return metaPath, logPath
}

func pickFirstTbl(path string) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "hashtbl-") && !strings.Contains(entry.Name(), "tmp") && !strings.Contains(entry.Name(), "temp") {
			return filepath.Join(path, entry.Name()), nil
		}
	}
	return "", errors.New("no hashtbl found in directory")
}
