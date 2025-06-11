package hashstore

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/hashstore"
	"strings"
	"time"
)

type Audit struct {
	Prefix    string
	Hashstore string `help:"the location of the hashstore files" default:"."`
}

func (a *Audit) Run() error {
	tables, err := listFiles(a.Hashstore, "hashtbl-", "")
	if err != nil {
		return errors.WithStack(err)
	}
	pieceFiles, err := listFiles(".", a.Prefix+"-", "")
	if err != nil {
		return errors.WithStack(err)
	}

	fmt.Println("Found hashtbl files:", tables)
	fmt.Println("Found piece files:", pieceFiles)

	var tbls []hashstore.Tbl
	var fls []io.Closer
	defer func() {
		for _, f := range fls {
			_ = f.Close()
		}
	}()
	ctx := context.Background()
	for _, table := range tables {
		f, err := os.Open(filepath.Join(a.Hashstore, table))
		if err != nil {
			return errors.WithStack(err)
		}
		fls = append(fls, f)

		tbl, err := hashstore.OpenTable(ctx, f)
		if err != nil {
			return errors.WithStack(err)
		}
		tbls = append(tbls, tbl)

	}

	output, err := os.Create(a.Prefix + ".notfound")
	if err != nil {
		return errors.WithStack(err)
	}
	defer output.Close()
	for _, pieceFile := range pieceFiles {
		err := auditFiles(output, tbls, pieceFile)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func auditFiles(output *os.File, tbls []hashstore.Tbl, file string) error {
	f, err := os.Open(file)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	ctx := context.Background()
	ix := 0
	lastReport := time.Now()
	defer func() {
		fmt.Println("Processed", ix, "lines in file", file)
	}()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) == 0 {
			continue
		}

		pieceIDStr := fields[len(fields)-1]

		pieceID, err := storj.PieceIDFromString(pieceIDStr)
		if err != nil {
			fmt.Printf("Failed to parse piece ID %s in file %s: %v\n", pieceIDStr, file, err)
			continue
		}

		found := false
		for _, tbl := range tbls {
			_, exists, err := tbl.Lookup(ctx, pieceID)
			if err != nil {
				return errors.WithStack(err)
			}
			if exists {
				found = true
				break
			}
		}

		if !found {
			_, err := output.Write([]byte(line + "\n"))
			if err != nil {
				return errors.WithStack(err)
			}
		}
		if time.Since(lastReport) > 5*time.Second {
			fmt.Printf("Processed %d lines in file %s\n", ix, file)
			lastReport = time.Now()
		}
		ix++
	}

	if err := scanner.Err(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func listFiles(dir string, prefix string, suffix string) (res []string, err error) {
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if (prefix == "" || strings.HasPrefix(info.Name(), prefix)) && (suffix == "" || strings.HasSuffix(info.Name(), suffix)) {
			res = append(res, info.Name())
		}
		return nil
	})
	return res, err
}
