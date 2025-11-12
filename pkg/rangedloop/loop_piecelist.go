package rangedloop

import (
	"os"
	"strings"

	"github.com/zeebo/errs"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
)

type PieceListLoop struct {
	WithRangedLoop
	NodeID string `help:"set a NodeID to generate a piece list report"`
}

func (p PieceListLoop) Run() error {
	nodeIDs, err := p.parseNodeIDs()
	if err != nil {
		return err
	}
	return p.RunLoop(func(observers []rangedloop.Observer) []rangedloop.Observer {
		return append(observers, NewPieceList(nodeIDs))
	})
}

func (p PieceListLoop) parseNodeIDs() ([]storj.NodeID, error) {
	// Check if NodeID parameter points to a file
	if fileInfo, err := os.Stat(p.NodeID); err == nil && !fileInfo.IsDir() {
		// Read nodeIDs from file
		content, err := os.ReadFile(p.NodeID)
		if err != nil {
			return nil, errs.New("Error reading nodeIDs file %s: %+v", p.NodeID, err)
		}

		var nodeIDs []storj.NodeID
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue // Skip empty lines and comments
			}
			nodeID, err := storj.NodeIDFromString(line)
			if err != nil {
				return nil, errs.New("Invalid nodeID in file %s: %s: %+v", p.NodeID, line, err)
			}
			nodeIDs = append(nodeIDs, nodeID)
		}
		return nodeIDs, nil
	} else {
		// Try to parse as single nodeID
		nodeID, err := storj.NodeIDFromString(p.NodeID)
		if err != nil {
			return nil, errs.New("Invalid nodeID %s: %+v", p.NodeID, err)
		}
		return []storj.NodeID{nodeID}, nil
	}
}
