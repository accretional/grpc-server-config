// cmd/validate runs GCP fixture validation tests against our metric aggregation
// implementation. It can also refresh the saved testdata from GCP.
//
// Usage:
//
//	go run ./cmd/validate              # run fixture tests
//	go run ./cmd/validate -refresh     # fetch fresh testdata, then run tests
//
// Flags:
//
//	-refresh           Fetch fresh testdata from GCP before running tests
//	-project PROJECT   GCP project ID (default: active gcloud project)
//	-hours N           Look-back window in hours for refresh (default: 1)
//	-dir PATH          Validation data directory (default: data/validation)
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	refresh := flag.Bool("refresh", false, "fetch fresh testdata from GCP before running tests")
	project := flag.String("project", "", "GCP project ID (default: active gcloud project)")
	hours := flag.Float64("hours", 1, "look-back window in hours (used with -refresh)")
	dir := flag.String("dir", "data/validation", "testdata directory")
	flag.Parse()

	if *refresh {
		runRefresh(*project, *hours, *dir)
	}

	runTests()
}

// runRefresh fetches all GCP fixture combinations and writes them to dir.
func runRefresh(project string, hours float64, dir string) {
	if project == "" {
		out, err := exec.Command("gcloud", "config", "get-value", "project").Output()
		if err != nil || strings.TrimSpace(string(out)) == "" {
			log.Fatal("no GCP project — pass -project=PROJECT or run 'gcloud config set project PROJECT'")
		}
		project = strings.TrimSpace(string(out))
	}

	end := time.Now().UTC().Truncate(time.Minute)
	start := end.Add(-time.Duration(float64(time.Hour) * hours))

	fetch := func(label, metricType, aligner, reducer string, groupBy []string, periodSecs int, overrideStart time.Time) {
		s := start
		if !overrideStart.IsZero() {
			s = overrideStart
		}
		suffix := "raw"
		if aligner != "" {
			suffix = fmt.Sprintf("%s_%ds", aligner, periodSecs)
		}
		if reducer != "" {
			suffix = fmt.Sprintf("%s_%s_%ds", aligner, reducer, periodSecs)
		}
		path := dataPath(dir, metricType, suffix)
		fmt.Printf("  %-60s → %s\n", label, path)
		if err := fetchAndSave(project, metricType, aligner, reducer, groupBy, periodSecs, s, end, path); err != nil {
			log.Fatalf("fetch %s: %v", label, err)
		}
	}

	sep := func(label string) { fmt.Printf("\n── %s ──\n", label) }

	cpu := "compute.googleapis.com/instance/cpu/utilization"
	sep("GAUGE  " + cpu)
	fetch("raw", cpu, "", "", nil, 60, time.Time{})
	for _, a := range []string{
		"ALIGN_MEAN", "ALIGN_MIN", "ALIGN_MAX", "ALIGN_COUNT", "ALIGN_SUM", "ALIGN_STDDEV",
		"ALIGN_INTERPOLATE", "ALIGN_NEXT_OLDER",
	} {
		fetch(a+"/60s", cpu, a, "", nil, 60, time.Time{})
	}
	fetch("ALIGN_PERCENT_CHANGE/60s", cpu, "ALIGN_PERCENT_CHANGE", "", nil, 60,
		start.Add(-1200*time.Second))
	fetch("ALIGN_MEAN/10s", cpu, "ALIGN_MEAN", "", nil, 10, time.Time{})
	for _, r := range []string{"REDUCE_MEAN", "REDUCE_MIN", "REDUCE_MAX", "REDUCE_SUM", "REDUCE_COUNT", "REDUCE_STDDEV"} {
		fetch("ALIGN_MEAN + "+r+"/zone", cpu, "ALIGN_MEAN", r, []string{"metric.labels.instance_name"}, 60, time.Time{})
	}

	disk := "compute.googleapis.com/instance/disk/write_ops_count"
	sep("DELTA  " + disk)
	fetch("raw", disk, "", "", nil, 60, time.Time{})
	for _, a := range []string{"ALIGN_RATE", "ALIGN_DELTA", "ALIGN_MEAN", "ALIGN_MIN", "ALIGN_MAX", "ALIGN_COUNT", "ALIGN_SUM", "ALIGN_STDDEV"} {
		fetch(a+"/60s", disk, a, "", nil, 60, time.Time{})
	}

	mem := "compute.googleapis.com/instance/memory/balloon/ram_used"
	sep("GAUGE  " + mem)
	fetch("raw", mem, "", "", nil, 60, time.Time{})
	fetch("ALIGN_MEAN/60s", mem, "ALIGN_MEAN", "", nil, 60, time.Time{})

	net := "compute.googleapis.com/instance/network/received_bytes_count"
	sep("DELTA  " + net)
	fetch("raw", net, "", "", nil, 60, time.Time{})
	fetch("ALIGN_RATE/60s", net, "ALIGN_RATE", "", nil, 60, time.Time{})

	upt := "compute.googleapis.com/instance/uptime"
	sep("DELTA  " + upt)
	fetch("raw", upt, "", "", nil, 60, time.Time{})
	fetch("ALIGN_RATE/60s", upt, "ALIGN_RATE", "", nil, 60, time.Time{})
	fetch("ALIGN_DELTA/60s", upt, "ALIGN_DELTA", "", nil, 60, time.Time{})

	usage := "compute.googleapis.com/instance/cpu/usage_time"
	sep("CUMULATIVE  " + usage)
	fetch("raw", usage, "", "", nil, 60, time.Time{})
	fetch("ALIGN_RATE/60s", usage, "ALIGN_RATE", "", nil, 60, time.Time{})
	fetch("ALIGN_DELTA/60s", usage, "ALIGN_DELTA", "", nil, 60, time.Time{})

	fmt.Printf("\nRefresh complete. Fixtures written to %s/\n\n", dir)
}

// runTests shells out to `go test` so the standard test framework handles
// fixture loading, diffs, and pass/fail reporting.
func runTests() {
	cmd := exec.Command("go", "test", "./cmd/validate/", "-run", "TestGCP_", "-v", "-timeout", "120s")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}
