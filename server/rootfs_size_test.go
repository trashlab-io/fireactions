package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRootFSSize(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want uint64
	}{
		{name: "gibibytes", raw: "100G", want: 100 * 1024 * 1024 * 1024},
		{name: "tebibytes", raw: "1T", want: 1024 * 1024 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRootFSSize(tt.raw)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseRootFSSizeRejectsInvalidSizes(t *testing.T) {
	for _, raw := range []string{"", "0G", "513B"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseRootFSSize(raw)
			require.Error(t, err)
		})
	}
}

func TestDevmapperDeviceName(t *testing.T) {
	got, err := devmapperDeviceName("/dev/mapper/containerd-thinpool-snap-13046")
	require.NoError(t, err)
	assert.Equal(t, "containerd-thinpool-snap-13046", got)
}

func TestDevmapperDeviceNameRejectsUnexpectedPaths(t *testing.T) {
	for _, device := range []string{"/dev/sda1", "/dev/mapper/", "/dev/mapper/nested/device"} {
		t.Run(device, func(t *testing.T) {
			_, err := devmapperDeviceName(device)
			require.Error(t, err)
		})
	}
}

func TestGrowDevmapperThinTable(t *testing.T) {
	const table = "0 209715200 thin /dev/mapper/containerd-thinpool 60000"

	got, err := growDevmapperThinTable(table, 1024*1024*1024*1024)
	require.NoError(t, err)
	assert.Equal(t, "0 2147483648 thin /dev/mapper/containerd-thinpool 60000", got)
}

func TestGrowDevmapperThinTableNoopsWhenAlreadyTargetSize(t *testing.T) {
	got, err := growDevmapperThinTable("0 2147483648 thin 254:2 55", 1024*1024*1024*1024)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestGrowDevmapperThinTableRejectsShrink(t *testing.T) {
	_, err := growDevmapperThinTable("0 2147483648 thin 254:2 55", 100*1024*1024*1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already larger")
}

func TestGrowDevmapperThinTableRejectsUnexpectedTargets(t *testing.T) {
	for _, table := range []string{
		"",
		"0 209715200 linear 254:2 0",
		"1 209715200 thin 254:2 55",
		"0 nope thin 254:2 55",
	} {
		t.Run(table, func(t *testing.T) {
			_, err := growDevmapperThinTable(table, 1024*1024*1024*1024)
			require.Error(t, err)
		})
	}
}
