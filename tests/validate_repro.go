package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) != 2 {
		fatalf("usage: validate_repro LOG")
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fatalf("open reproducer log: %v", err)
	}
	defer f.Close()

	seen := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	for line := 1; scanner.Scan(); line++ {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
			fatalf("line %d is not valid JSON: %v", line, err)
		}
		if len(raw) != 4 {
			fatalf("line %d has %d keys, want exactly 4", line, len(raw))
		}
		for _, key := range []string{"scenario", "ok", "txid", "detail"} {
			if _, ok := raw[key]; !ok {
				fatalf("line %d is missing key %q", line, key)
			}
		}

		var scenario, txid, detail string
		var ok bool
		if err := json.Unmarshal(raw["scenario"], &scenario); err != nil || scenario == "" {
			fatalf("line %d has invalid scenario", line)
		}
		if err := json.Unmarshal(raw["ok"], &ok); err != nil || !ok {
			fatalf("line %d is not a passing scenario", line)
		}
		if err := json.Unmarshal(raw["txid"], &txid); err != nil {
			fatalf("line %d has non-string txid", line)
		}
		if _, err := strconv.ParseUint(txid, 10, 64); err != nil {
			fatalf("line %d has invalid unsigned txid %q", line, txid)
		}
		if err := json.Unmarshal(raw["detail"], &detail); err != nil || detail == "" {
			fatalf("line %d has invalid detail", line)
		}
		if _, duplicate := seen[scenario]; duplicate {
			fatalf("line %d repeats scenario %q", line, scenario)
		}
		seen[scenario] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		fatalf("read reproducer log: %v", err)
	}
	if len(seen) < 3 {
		fatalf("reproducer emitted %d unique scenarios, want at least 3", len(seen))
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
