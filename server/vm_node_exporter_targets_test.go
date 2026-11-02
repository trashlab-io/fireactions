package server

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/firecracker-microvm/firecracker-go-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolWritesAndRemovesVMNodeExporterTarget(t *testing.T) {
	targetsDir := t.TempDir()
	pool := newTestPool(t, "pool/a", 2048, 2, NewCapacityManager(nil))
	pool.vmNodeExporterConfig = &VMNodeExporterConfig{
		Enabled:    true,
		Port:       9100,
		TargetsDir: targetsDir,
	}

	runtime := newFakeMachineRuntime()
	machine := &Machine{
		Machine:      newTestFirecrackerMachineWithIP("192.168.128.42"),
		Name:         "runner/1",
		Pool:         pool.config.Name,
		Organization: "test-org",
		WorkflowName: "CI",
		JobName:      "Unit Tests",
		CreatedAt:    time.Now().UTC(),
		waitFunc:     runtime.wait,
		stopFunc:     runtime.stop,
	}

	pool.trackMachine(machine)

	targetPath := filepath.Join(targetsDir, "fireactions-vm-pool_a-runner_1.node-exporter.yml")
	data, err := os.ReadFile(targetPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "192.168.128.42:9100")
	assert.Contains(t, string(data), "pool: pool/a")
	assert.Contains(t, string(data), "runner: runner/1")
	assert.Contains(t, string(data), "role: vm")
	assert.Contains(t, string(data), "workflow: CI")
	assert.Contains(t, string(data), "workflow_job: Unit Tests")

	runtime.stop()
	require.Eventually(t, func() bool {
		_, err := os.Stat(targetPath)
		return os.IsNotExist(err)
	}, time.Second, 10*time.Millisecond)
}

func TestClearVMNodeExporterTargetsOnlyRemovesFireactionsVMFiles(t *testing.T) {
	targetsDir := t.TempDir()
	stalePath := filepath.Join(targetsDir, "fireactions-vm-pool-runner.node-exporter.yml")
	otherPath := filepath.Join(targetsDir, "client.node-exporter.yml")
	require.NoError(t, os.WriteFile(stalePath, []byte("stale"), 0644))
	require.NoError(t, os.WriteFile(otherPath, []byte("keep"), 0644))

	err := clearVMNodeExporterTargets(&VMNodeExporterConfig{
		Enabled:    true,
		Port:       9100,
		TargetsDir: targetsDir,
	})
	require.NoError(t, err)

	_, err = os.Stat(stalePath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(otherPath)
	require.NoError(t, err)
}

func newTestFirecrackerMachineWithIP(ip string) *firecracker.Machine {
	return &firecracker.Machine{
		Cfg: firecracker.Config{
			NetworkInterfaces: []firecracker.NetworkInterface{{
				StaticConfiguration: &firecracker.StaticNetworkConfiguration{
					IPConfiguration: &firecracker.IPConfiguration{
						IPAddr: net.IPNet{IP: net.ParseIP(ip), Mask: net.CIDRMask(24, 32)},
					},
				},
			}},
		},
	}
}
