package interceptor

import (
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/accretional/grpc-server-config/pb/metrics"
)

// EnableSysctl starts a background goroutine that samples the given sysctl
// keys every sampleInterval and writes them to the metrics store.
//
// Each key becomes its own metric type under resource type "host":
//
//	fetch host::kern.ipc.somaxconn | within 1h
//
// Keys that cannot be read or parsed as numbers are skipped with a log warning.
// Call this before the server starts serving traffic.
func (i *Interceptor) EnableSysctl(keys []string) {
	go i.sysctlLoop(keys)
}

func (i *Interceptor) sysctlLoop(keys []string) {
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	i.sampleSysctl(keys) // capture one sample immediately
	for {
		select {
		case <-ticker.C:
			i.sampleSysctl(keys)
		case <-i.stopCh:
			return
		}
	}
}

func (i *Interceptor) sampleSysctl(keys []string) {
	now := timestamppb.Now()
	res := &pb.Resource{Type: resourceTypeHost, Name: i.service}
	labels := map[string]string{}

	for _, key := range keys {
		val, err := readSysctl(key)
		if err != nil {
			log.Printf("interceptor: sysctl %s: %v", key, err)
			continue
		}
		i.send(gaugeDouble(key, labels, now, res, val))
	}
}

// readSysctl shells out to `sysctl -n <key>` and parses the result as float64.
// Works on macOS and Linux (key namespaces differ between the two).
func readSysctl(key string) (float64, error) {
	out, err := exec.Command("sysctl", "-n", key).Output()
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(out))
	return strconv.ParseFloat(s, 64)
}
