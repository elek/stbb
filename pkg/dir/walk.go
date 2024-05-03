package dir

import (
	"fmt"
	"github.com/pkg/errors"
	"io/fs"
	"path/filepath"
)

type Walk struct {
	Dir string `arg:""`
}

func (w Walk) Run() error {
	size := int64(0)
	err := filepath.WalkDir(w.Dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return errors.WithStack(err)
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Print(size)
	return nil
}
