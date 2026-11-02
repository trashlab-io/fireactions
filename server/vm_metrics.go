package server

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
)

const (
	vmMetricLabelPool         = "pool"
	vmMetricLabelOrganization = "organization"
	vmMetricLabelRunner       = "runner"
)

var (
	metricVMHostProcess = newVMHostMetricsCollector(procfs.DefaultMountPoint)
)

type vmHostMetricsCollector struct {
	procRoot string

	cpuSeconds   *prometheus.Desc
	memoryRSS    *prometheus.Desc
	configMemory *prometheus.Desc
	configVCPUs  *prometheus.Desc

	mu        sync.RWMutex
	processes map[string]trackedVMProcess
}

type trackedVMProcess struct {
	pid          int
	pool         string
	organization string
	runner       string
	memoryBytes  float64
	vcpuCount    float64
}

func newVMHostMetricsCollector(procRoot string) *vmHostMetricsCollector {
	labels := []string{vmMetricLabelPool, vmMetricLabelOrganization, vmMetricLabelRunner}

	return &vmHostMetricsCollector{
		procRoot: procRoot,
		cpuSeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "vm", "cpu_seconds_total"),
			"Cumulative host-observed CPU seconds consumed by the Firecracker process for an active VM.",
			labels,
			nil,
		),
		memoryRSS: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "vm", "memory_rss_bytes"),
			"Current host-observed RSS memory in bytes for the Firecracker process of an active VM.",
			labels,
			nil,
		),
		configMemory: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "vm", "configured_memory_bytes"),
			"Configured guest memory in bytes for an active VM.",
			labels,
			nil,
		),
		configVCPUs: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "vm", "configured_vcpus"),
			"Configured guest vCPU count for an active VM.",
			labels,
			nil,
		),
		processes: make(map[string]trackedVMProcess),
	}
}

func (c *vmHostMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.cpuSeconds
	ch <- c.memoryRSS
	ch <- c.configMemory
	ch <- c.configVCPUs
}

func (c *vmHostMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	processes := c.snapshot()

	fs, err := procfs.NewFS(c.procRoot)
	if err != nil {
		return
	}

	for _, process := range processes {
		proc, err := fs.Proc(process.pid)
		if err != nil {
			continue
		}

		stat, err := proc.Stat()
		if err != nil {
			continue
		}

		statm, err := proc.Statm()
		if err != nil {
			continue
		}

		labelValues := []string{process.pool, process.organization, process.runner}
		ch <- prometheus.MustNewConstMetric(c.cpuSeconds, prometheus.CounterValue, stat.CPUTime(), labelValues...)
		ch <- prometheus.MustNewConstMetric(c.memoryRSS, prometheus.GaugeValue, float64(statm.ResidentBytes()), labelValues...)
		ch <- prometheus.MustNewConstMetric(c.configMemory, prometheus.GaugeValue, process.memoryBytes, labelValues...)
		ch <- prometheus.MustNewConstMetric(c.configVCPUs, prometheus.GaugeValue, process.vcpuCount, labelValues...)
	}
}

func (c *vmHostMetricsCollector) track(machine *Machine) {
	if machine == nil || machine.ProcessID <= 0 {
		return
	}

	process := trackedVMProcess{
		pid:          machine.ProcessID,
		pool:         machine.Pool,
		organization: machine.Organization,
		runner:       machine.Name,
		memoryBytes:  float64(machine.MemoryMib * 1024 * 1024),
		vcpuCount:    float64(machine.VCPUCount),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.processes[process.key()] = process
}

func (c *vmHostMetricsCollector) untrack(machine *Machine) {
	if machine == nil {
		return
	}

	process := trackedVMProcess{
		pool:         machine.Pool,
		organization: machine.Organization,
		runner:       machine.Name,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.processes, process.key())
}

func (c *vmHostMetricsCollector) snapshot() []trackedVMProcess {
	c.mu.RLock()
	defer c.mu.RUnlock()

	processes := make([]trackedVMProcess, 0, len(c.processes))
	for _, process := range c.processes {
		processes = append(processes, process)
	}

	return processes
}

func (p trackedVMProcess) key() string {
	return p.pool + "\xff" + p.organization + "\xff" + p.runner
}

func init() {
	prometheus.MustRegister(metricVMHostProcess)
}
