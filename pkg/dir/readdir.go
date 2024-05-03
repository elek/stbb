package dir

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

type ReadDir struct {
	Dir string `arg:""`
}

func (w ReadDir) Run() error {
	size := int64(0)
	dirs, err := os.ReadDir(w.Dir)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}

		dirPath := filepath.Join(w.Dir, d.Name())
		files, err := os.ReadDir(dirPath)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			info, err := f.Info()
			if err != nil {
				return errors.WithStack(err)
			}
			size += info.Size()
		}

	}
	fmt.Print(size)
	return nil
}
