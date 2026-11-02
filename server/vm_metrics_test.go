package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestVMHostMetricsCollectorCollectsActiveProcessMetrics(t *testing.T) {
	procRoot := t.TempDir()
	writeTestProc(t, procRoot, 1234, 200, 50, 42)

	collector := newVMHostMetricsCollector(procRoot)
	collector.track(&Machine{
		Name:         "runner-1",
		Pool:         "pool-a",
		Organization: "org-a",
		ProcessID:    1234,
		MemoryMib:    2048,
		VCPUCount:    2,
	})

	expected := fmt.Sprintf(`
# HELP fireactions_vm_configured_memory_bytes Configured guest memory in bytes for an active VM.
# TYPE fireactions_vm_configured_memory_bytes gauge
fireactions_vm_configured_memory_bytes{organization="org-a",pool="pool-a",runner="runner-1"} 2147483648
# HELP fireactions_vm_configured_vcpus Configured guest vCPU count for an active VM.
# TYPE fireactions_vm_configured_vcpus gauge
fireactions_vm_configured_vcpus{organization="org-a",pool="pool-a",runner="runner-1"} 2
# HELP fireactions_vm_cpu_seconds_total Cumulative host-observed CPU seconds consumed by the Firecracker process for an active VM.
# TYPE fireactions_vm_cpu_seconds_total counter
fireactions_vm_cpu_seconds_total{organization="org-a",pool="pool-a",runner="runner-1"} 2.5
# HELP fireactions_vm_memory_rss_bytes Current host-observed RSS memory in bytes for the Firecracker process of an active VM.
# TYPE fireactions_vm_memory_rss_bytes gauge
fireactions_vm_memory_rss_bytes{organization="org-a",pool="pool-a",runner="runner-1"} %d
`, 42*os.Getpagesize())

	require.NoError(t, testutil.CollectAndCompare(
		collector,
		strings.NewReader(expected),
		"fireactions_vm_configured_memory_bytes",
		"fireactions_vm_configured_vcpus",
		"fireactions_vm_cpu_seconds_total",
		"fireactions_vm_memory_rss_bytes",
	))
}

func TestVMHostMetricsCollectorUntrackRemovesSeries(t *testing.T) {
	procRoot := t.TempDir()
	writeTestProc(t, procRoot, 1234, 200, 50, 42)

	collector := newVMHostMetricsCollector(procRoot)
	machine := &Machine{
		Name:         "runner-1",
		Pool:         "pool-a",
		Organization: "org-a",
		ProcessID:    1234,
		MemoryMib:    2048,
		VCPUCount:    2,
	}

	collector.track(machine)
	require.Equal(t, 4, testutil.CollectAndCount(collector))

	collector.untrack(machine)
	require.Equal(t, 0, testutil.CollectAndCount(collector))
}

func writeTestProc(t *testing.T, procRoot string, pid int, utime, stime, rssPages uint64) {
	t.Helper()

	procDir := filepath.Join(procRoot, fmt.Sprintf("%d", pid))
	require.NoError(t, os.MkdirAll(procDir, 0755))

	statFields := []string{
		"S",
		"1", "1", "1", "0", "0", "0", "0", "0", "0", "0",
		fmt.Sprintf("%d", utime),
		fmt.Sprintf("%d", stime),
		"0", "0", "20", "0", "1", "0", "100",
		"4096", fmt.Sprintf("%d", rssPages), "18446744073709551615",
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0", "0", "0",
	}
	require.Len(t, statFields, 42)

	stat := fmt.Sprintf("%d (firecracker) %s\n", pid, strings.Join(statFields, " "))
	require.NoError(t, os.WriteFile(filepath.Join(procDir, "stat"), []byte(stat), 0644))

	statm := fmt.Sprintf("100 %d 0 0 0 0 0\n", rssPages)
	require.NoError(t, os.WriteFile(filepath.Join(procDir, "statm"), []byte(statm), 0644))
}
