package nodeselection

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
	"io"
	"math/rand"
	"os"
	"sort"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metainfo"
	"storj.io/storj/satellite/nodeselection"
	"strconv"
	"strings"
)

type Simulator struct {
	Nodes            string `help:"csv file with the node table export"`
	Performance      string `help:"csv file with the node performance data"`
	Selector         string `help:"selector definition" default:"random()"`
	Selection        int    `help:"number of selection to be simulated" default:"100000"`
	NodeCacheRefresh int    `help:"number of samples after node cache is refreshed (only important for filterbest)." default:"10000"`
	selected         map[storj.NodeID]int
	used             map[storj.NodeID]int
	performance      map[storj.NodeID]func() int
	duration         []int
	tracker          metainfo.SuccessTracker
}

func (s *Simulator) Get(uplink storj.NodeID) func(node storj.NodeID) float64 {
	return s.tracker.Get
}

func (s *Simulator) Run() error {
	s.used = make(map[storj.NodeID]int)
	s.selected = make(map[storj.NodeID]int)

	trackerC, known := metainfo.GetNewSuccessTracker("bitshift")
	if !known {
		return errors.New("Unknown tracker type")
	}
	s.tracker = trackerC()

	selectorInit, err := nodeselection.SelectorFromString(s.Selector, nodeselection.NewPlacementConfigEnvironment(s))
	if err != nil {
		return errors.WithStack(err)
	}

	nodes, err := s.ReadNodes()
	if err != nil {
		return errors.WithStack(err)
	}

	err = s.ReadPerformanceData(nodes)
	if err != nil {
		return errors.WithStack(err)
	}

	var filter nodeselection.NodeFilter

	selector := selectorInit(nodes, filter)
	selection := s.Selection
	rps := 4000
	for i := 0; i < selection; i++ {
		if i%(rps*60) == 0 {
			s.tracker.BumpGeneration()
		}
		if i%s.NodeCacheRefresh == 0 {
			selector = selectorInit(nodes, filter)
		}
		nodesToSelect := 110
		selectedNodes, err := selector(storj.NodeID{}, nodesToSelect, nil, nil)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(selectedNodes) < nodesToSelect {
			return errors.New(fmt.Sprintf("Not enough node %d %d", len(selectedNodes), nodesToSelect))
		}

		uploadTimes := make(map[storj.NodeID]int)
		for _, node := range selectedNodes {
			uploadTimes[node.ID] = s.getUploadTimeMs(node.ID)
		}
		slices.SortFunc(selectedNodes, func(a, b *nodeselection.SelectedNode) int {
			atime := uploadTimes[a.ID]
			btime := uploadTimes[b.ID]
			if atime == btime {
				return 0
			}
			if atime < btime {
				return -1
			}
			return 1
		})

		for ix, node := range selectedNodes {
			if ix < 80 {
				s.tracker.Increment(node.ID, true)
				s.used[node.ID]++
			} else {
				s.tracker.Increment(node.ID, false)
			}
			s.selected[node.ID]++
		}
		s.duration = append(s.duration, uploadTimes[selectedNodes[80].ID])
	}

	fmt.Println("selection number", selection)
	fmt.Println("node number", len(nodes))
	fmt.Println()
	fmt.Println("selected")
	stat(mapToSlice(s.selected))
	fmt.Println()
	fmt.Println("used")
	stat(mapToSlice(s.used))
	fmt.Println()
	fmt.Println("duration")
	stat(s.duration)
	return nil
}

func mapToSlice(source map[storj.NodeID]int) []int {
	var numbers []int
	for _, v := range source {
		numbers = append(numbers, v)
	}
	return numbers
}

func stat(numbers []int) {
	sort.Ints(numbers)
	fmt.Println("   MIN", numbers[0])
	for i := 1; i < 10; i++ {
		fmt.Printf("   P%d %d\n", i*10, numbers[len(numbers)/10*i-1])
	}
	fmt.Println("   P95", numbers[len(numbers)/100*95-1])
	fmt.Println("   P99", numbers[len(numbers)/100*99-1])
	fmt.Println("   MAX", numbers[len(numbers)-1])
}

func (s *Simulator) getUploadTimeMs(id storj.NodeID) int {
	perf, found := s.performance[id]
	if !found {
		return 1000
	}
	return perf()

}

func (s *Simulator) ReadPerformanceData(nodes []*nodeselection.SelectedNode) error {
	input, err := os.ReadFile(s.Performance)
	if err != nil {
		return errors.WithStack(err)
	}

	data := []PerformanceRecord{}
	err = json.Unmarshal(input, &data)
	if err != nil {
		return errors.WithStack(err)
	}
	s.performance = make(map[storj.NodeID]func() int)

	for _, d := range data {
		if d.TagField != "ravg" {
			continue
		}
		id, err := storj.NodeIDFromString(d.TagInstance)
		if err != nil {
			var found bool
			parts := strings.Split(d.TagInstance, "-")
			for _, n := range nodes {
				if strings.HasPrefix(n.ID.String(), parts[len(parts)-1]) {
					found = true
					id = n.ID
					break
				}
			}
			if !found {
				fmt.Println("Wrong instance", d.TagInstance)
				continue
			}
		}

		min, err := strconv.Atoi(strings.Split(d.Minimum, ".")[0])
		if err != nil {
			return errors.WithStack(err)
		}
		max, err := strconv.Atoi(strings.Split(d.Maximum, ".")[0])
		if err != nil {
			return errors.WithStack(err)
		}
		min = min / 1000 / 1000
		max = max / 1000 / 1000
		s.performance[id] = func() int {

			if max <= min {
				return min
			}
			val := min + rand.Intn(max-min)
			return val
		}

	}
	return nil
}

func (s *Simulator) ReadNodes() (nodes []*nodeselection.SelectedNode, err error) {
	input, err := os.Open(s.Nodes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer input.Close()
	csvReader := csv.NewReader(input)
	header, err := csvReader.Read()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	indexes := map[string]int{}
	for ix, name := range header {
		indexes[name] = ix
	}

	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		id, err := hex.DecodeString(line[indexes["id"]])
		if err != nil {
			return nil, errors.WithStack(err)
		}

		nodeID, err := storj.NodeIDFromBytes(id)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		sn := &nodeselection.SelectedNode{
			ID:         nodeID,
			LastIPPort: line[indexes["last_ip_port"]],
			LastNet:    line[indexes["last_net"]],
			Vetted:     line[indexes["vetted_at"]] != "",
		}
		nodes = append(nodes, sn)
	}
	return nodes, nil
}

type PerformanceRecord struct {
	TagInstance string `json:"tag_instance"`
	TagField    string `json:"tag_field"`
	Minimum     string `json:"minimum"`
	Maximum     string `json:"maximum"`
	Average     string `json:"average"`
}
