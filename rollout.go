package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
)

type Rollout struct {
}

func (r Rollout) Run() error {
	resp, err := http.Get("https://version.storj.io/")
	if err != nil {
		return fmt.Errorf("failed to download versions: %w", err)
	}
	defer resp.Body.Close()

	var versions Versions
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		return fmt.Errorf("failed to parse versions: %w", err)
	}

	maxCursor := new(big.Int)
	maxCursor.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	for name, process := range versions.Processes {
		cursorInt, ok := new(big.Int).SetString(process.Rollout.Cursor, 16)
		if !ok {
			continue
		}

		fmt.Printf("Process: %s\n", name)
		fmt.Printf("Minimum: %s\n", process.Minimum.Version)
		fmt.Printf("Suggested: %s\n", process.Suggested.Version)
		fmt.Printf("  Cursor: %s\n", process.Rollout.Cursor)

		if maxCursor.Cmp(big.NewInt(0)) == 0 {
			fmt.Printf("  Error: maxCursor is zero, cannot calculate percentage\n")
			continue
		}

		cursorFloat := new(big.Float).SetInt(cursorInt)
		maxCursorFloat := new(big.Float).SetInt(maxCursor)
		percentageFloat := new(big.Float).Quo(cursorFloat, maxCursorFloat)
		percentageFloat.Mul(percentageFloat, big.NewFloat(100))

		fmt.Printf("  Percentage: %f%%\n", percentageFloat)
		fmt.Println()
	}

	return nil
}

type Versions struct {
	Processes map[string]RolloutInfo `json:"processes"`
}

type RolloutInfo struct {
	Minimum struct {
		Version string `json:"version"`
		Url     string `json:"url"`
	} `json:"minimum"`
	Suggested struct {
		Version string `json:"version"`
		Url     string `json:"url"`
	} `json:"suggested"`
	Rollout struct {
		Seed   string `json:"seed"`
		Cursor string `json:"cursor"`
	} `json:"rollout"`
}
