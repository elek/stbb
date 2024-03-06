package segment

import (
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"storj.io/infectious"
	"storj.io/uplink/private/eestream"
	"strconv"
	"strings"
)

type ECDecode struct {
	StreamID string `arg:""`
}

func (s *ECDecode) Run() error {
	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	fec, err := infectious.NewFEC(29, 110)
	if err != nil {
		return err
	}

	shares := []infectious.Share{}

	segmentDir := fmt.Sprintf("segment_%s_%d", su, sp.Encode())
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(segmentDir, e.Name()))
		if err != nil {
			return err
		}
		parts := strings.Split(e.Name(), "_")
		if len(parts) < 2 {
			continue
		}
		num, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		shares = append(shares, infectious.Share{
			Number: num,
			Data:   data,
		})
	}
	fmt.Println(len(shares), "shares are loaded")

	scheme := eestream.NewRSScheme(fec, 256)
	out := make([]byte, 0)
	decoded, err := scheme.Decode(out, shares)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.WriteFile(fmt.Sprintf("segment_%s_%d.bin", su, sp.Encode()), decoded, 0644)
	if err != nil {
		return err
	}
	return nil
}
