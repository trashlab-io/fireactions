package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	config, err := NewConfig("testdata/config1.yaml")
	require.NoError(t, err)

	assert.Equal(t, "testdata/config1.yaml", config.path)
	require.Len(t, config.Pools, 2)
	assert.NotEmpty(t, config.Pools[0].Firecracker.CPUConfig)
	assert.Empty(t, config.Pools[1].Firecracker.CPUConfig)
	require.Contains(t, config.Pools[0].Firecracker.CPUConfig, "cpuid_modifiers")
}

func TestConfigValidateCapacityDefaults(t *testing.T) {
	config := minimalValidConfig()

	require.NoError(t, config.Validate())
	require.NotNil(t, config.Capacity)
	require.NotNil(t, config.Metrics.VMNodeExporter)
	assert.Equal(t, int64(0), config.Capacity.MemoryLimitMib)
	assert.Equal(t, int64(0), config.Capacity.VCPULimit)
	assert.Equal(t, 9100, config.Metrics.VMNodeExporter.Port)
}

func TestConfigValidateRejectsNegativeCapacity(t *testing.T) {
	config := minimalValidConfig()
	config.Capacity = &CapacityConfig{
		MemoryLimitMib: -1,
		VCPULimit:      -1,
	}

	err := config.Validate()
	require.Error(t, err)
}

func TestConfigValidateRejectsInvalidMachineShape(t *testing.T) {
	config := minimalValidConfig()
	config.Pools[0].Firecracker.MachineConfig.MemSizeMib = 0

	err := config.Validate()
	require.Error(t, err)

	config = minimalValidConfig()
	config.Pools[0].Firecracker.MachineConfig.VcpuCount = 0

	err = config.Validate()
	require.Error(t, err)
}

func TestConfigValidateRejectsPoolLargerThanGlobalCapacity(t *testing.T) {
	config := minimalValidConfig()
	config.Capacity = &CapacityConfig{
		MemoryLimitMib: 1024,
		VCPULimit:      1,
	}

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds capacity")
}

func TestConfigValidateRejectsNilPoolEntry(t *testing.T) {
	config := minimalValidConfig()
	config.Pools = []*PoolConfig{nil}

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool at index 0 is required")
}

func TestConfigValidateOnDemandRequiresWebhookSecret(t *testing.T) {
	config := minimalValidConfig()
	config.OnDemand = true

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "webhook_secret")
}

func TestConfigValidateOnDemandRequiresPoolNameLabel(t *testing.T) {
	config := minimalValidConfig()
	config.OnDemand = true
	config.GitHub.WebhookSecret = "secret"
	config.Pools[0].Runner.Labels = []string{"self-hosted"}

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "runner.labels must include the pool name")
}

func TestConfigValidateVMNodeExporterRequiresTargetsDir(t *testing.T) {
	config := minimalValidConfig()
	config.Metrics.VMNodeExporter.Enabled = true

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metrics.vm_node_exporter.targets_dir")
}

func TestConfigValidateVMNodeExporterDefaultsPort(t *testing.T) {
	config := minimalValidConfig()
	config.Metrics.VMNodeExporter = &VMNodeExporterConfig{
		Enabled:    true,
		TargetsDir: "/var/lib/fireactions/prometheus-targets",
	}

	require.NoError(t, config.Validate())
	assert.Equal(t, 9100, config.Metrics.VMNodeExporter.Port)
}

func minimalValidConfig() *Config {
	config := DefaultConfig()
	config.BindAddress = "127.0.0.1:8080"
	config.GitHub = &GitHubConfig{
		AppPrivateKey: "private-key",
		AppID:         1,
		WebhookSecret: "",
	}
	config.LogLevel = "info"
	config.Pools = []*PoolConfig{{
		Name:           "default",
		ShutdownOnExit: boolPtr(true),
		Replicas:       1,
		Runner: &RunnerConfig{
			Name:            "runner",
			ImagePullPolicy: "IfNotPresent",
			Image:           "ghcr.io/example/fireactions:test",
			Organization:    "example",
			GroupID:         1,
			Labels:          []string{"self-hosted", "default"},
		},
		Firecracker: &FirecrackerConfig{
			KernelImagePath: "/var/lib/fireactions/vmlinux",
			MachineConfig: FirecrackerMachineConfig{
				MemSizeMib: 2048,
				VcpuCount:  2,
			},
		},
	}}

	return config
}
