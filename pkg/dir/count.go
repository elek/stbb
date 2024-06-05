package dir

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
)

type Count struct {
	Count int
}

func (w Count) Run() error {
	dirs, err := os.ReadDir(".")
	if err != nil {
		return errors.WithStack(err)
	}
	for _, dir := range dirs {
		//if !dir.IsDir() {
		//	continue
		//}
		files, err := os.ReadDir(dir.Name())
		if err != nil {
			return errors.WithStack(err)
		}
		for range files {
			w.Count++
		}
	}
	fmt.Println(w.Count)
	return nil
}
