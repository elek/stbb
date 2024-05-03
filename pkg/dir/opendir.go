package dir

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

type OpenDir struct {
	Dir string `arg:""`
}

func (o OpenDir) Run() error {
	openDir, err := os.Open(o.Dir)
	defer openDir.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	var subdirNames []string
	for {
		names, err := openDir.Readdirnames(1024)
		if err != nil {
			if errors.Is(err, io.EOF) || os.IsNotExist(err) {
				break
			}
			return err
		}
		if len(names) == 0 {
			return nil
		}

		subdirNames = append(subdirNames, names...)
	}
	size := int64(0)
	for _, subdir := range subdirNames {
		s, err := o.SubdirSize(filepath.Join(o.Dir, subdir))
		if err != nil {
			fmt.Println(filepath.Join(o.Dir, subdir), err)
		} else {
			size += s
		}
	}
	fmt.Print(size)
	return nil
}

func (o OpenDir) SubdirSize(path string) (int64, error) {
	openDir, err := os.Open(path)
	defer openDir.Close()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	size := int64(0)
	for {
		names, err := openDir.Readdirnames(1024)
		if err != nil {
			if errors.Is(err, io.EOF) || os.IsNotExist(err) {
				break
			}
			return 0, err
		}
		if len(names) == 0 {
			break
		}
		for _, name := range names {
			stat, err := os.Stat(filepath.Join(path, name))
			if err != nil {
				fmt.Println(filepath.Join(path, name), err)
			} else {
				size += stat.Size()
			}
		}

	}
	return size, nil
}
