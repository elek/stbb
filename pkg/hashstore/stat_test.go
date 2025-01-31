package hashstore

import (
	"fmt"
	"storj.io/storj/storagenode/hashstore"
	"testing"
	"time"
)

func TestToday(t *testing.T) {
	fmt.Println(hashstore.TimeToDateDown(time.Now()))
}
