// cmd/compare fetches time series from GCP Cloud Monitoring and compares
// Google's aligned output against our AggregationService implementation.
//
// Two modes:
//
//	--fetch   Hit the GCP API and save raw + GCP-aligned data to --dir.
//	--compare Load saved data, run our aligner, diff against GCP's output.
//
// Typical workflow:
//
//	go run ./cmd/compare --fetch   --project=my-project --metric=.../cpu/utilization --aligner=ALIGN_MEAN
//	go run ./cmd/compare --compare --metric=.../cpu/utilization --aligner=ALIGN_MEAN
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/accretional/grpc-server-config/pb"
)

var alignerByName = map[string]pb.Aligner{
	"ALIGN_MEAN":           pb.Aligner_ALIGN_MEAN,
	"ALIGN_MIN":            pb.Aligner_ALIGN_MIN,
	"ALIGN_MAX":            pb.Aligner_ALIGN_MAX,
	"ALIGN_SUM":            pb.Aligner_ALIGN_SUM,
	"ALIGN_COUNT":          pb.Aligner_ALIGN_COUNT,
	"ALIGN_STDDEV":         pb.Aligner_ALIGN_STDDEV,
	"ALIGN_RATE":           pb.Aligner_ALIGN_RATE,
	"ALIGN_DELTA":          pb.Aligner_ALIGN_DELTA,
	"ALIGN_INTERPOLATE":    pb.Aligner_ALIGN_INTERPOLATE,
	"ALIGN_NEXT_OLDER":     pb.Aligner_ALIGN_NEXT_OLDER,
	"ALIGN_PERCENT_CHANGE": pb.Aligner_ALIGN_PERCENT_CHANGE,
}

func main() {
	project := flag.String("project", "scratchpad-427517", "GCP project ID")
	metric := flag.String("metric", "compute.googleapis.com/instance/cpu/utilization", "metric.type filter")
	aligner := flag.String("aligner", "ALIGN_MEAN", "per-series aligner (e.g. ALIGN_MEAN, ALIGN_RATE)")
	period := flag.Int("period", 60, "alignment period in seconds")
	hours := flag.Float64("hours", 1, "how many hours back to fetch")
	dir := flag.String("dir", "cmd/compare/testdata", "directory for saved JSON files")
	doFetch := flag.Bool("fetch", false, "fetch from GCP and save to --dir")
	doCompare := flag.Bool("compare", false, "compare our aligner against saved GCP data")
	flag.Parse()

	if !*doFetch && !*doCompare {
		fmt.Fprintln(os.Stderr, "specify --fetch and/or --compare")
		flag.Usage()
		os.Exit(1)
	}

	alignerEnum, ok := alignerByName[strings.ToUpper(*aligner)]
	if !ok {
		log.Fatalf("unknown aligner %q — valid: %v", *aligner, validAligners())
	}

	end := time.Now().UTC().Truncate(time.Minute)
	start := end.Add(-time.Duration(float64(time.Hour) * *hours))

	rawFile := dataPath(*dir, *metric, "raw")
	alignedFile := dataPath(*dir, *metric, fmt.Sprintf("%s_%ds", *aligner, *period))

	if *doFetch {
		log.Printf("fetching raw data  → %s", rawFile)
		t0 := time.Now()
		if err := fetchAndSave(*project, *metric, "", *period, start, end, rawFile); err != nil {
			log.Fatalf("fetch raw: %v", err)
		}
		log.Printf("  raw fetch latency: %s", time.Since(t0).Round(time.Millisecond))

		log.Printf("fetching aligned data (%s, %ds) → %s", *aligner, *period, alignedFile)
		t1 := time.Now()
		if err := fetchAndSave(*project, *metric, *aligner, *period, start, end, alignedFile); err != nil {
			log.Fatalf("fetch aligned: %v", err)
		}
		log.Printf("  aligned fetch latency: %s", time.Since(t1).Round(time.Millisecond))
		log.Printf("fetch complete")
	}

	if *doCompare {
		log.Printf("loading %s", rawFile)
		rawResp, err := loadResponse(rawFile)
		if err != nil {
			log.Fatalf("load raw: %v", err)
		}
		log.Printf("loading %s", alignedFile)
		gcpResp, err := loadResponse(alignedFile)
		if err != nil {
			log.Fatalf("load aligned: %v", err)
		}
		runComparison(rawResp, gcpResp, alignerEnum, time.Duration(*period)*time.Second, *aligner, *metric)
	}
}

// dataPath builds a deterministic filename for a saved response.
// e.g. "testdata/instance_cpu_utilization_raw.json"
func dataPath(dir, metricType, suffix string) string {
	// Strip common prefix and replace slashes/dots with underscores.
	name := strings.NewReplacer(
		"compute.googleapis.com/", "",
		"/", "_",
		".", "_",
	).Replace(metricType)
	return fmt.Sprintf("%s/%s_%s.json", dir, name, suffix)
}

func validAligners() []string {
	names := make([]string, 0, len(alignerByName))
	for k := range alignerByName {
		names = append(names, k)
	}
	return names
}
