package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const vmNodeExporterTargetSuffix = ".node-exporter.yml"

type prometheusFileSDTarget struct {
	Targets []string          `yaml:"targets"`
	Labels  map[string]string `yaml:"labels,omitempty"`
}

func clearVMNodeExporterTargets(config *VMNodeExporterConfig) error {
	if config == nil || !config.Enabled {
		return nil
	}

	if err := os.MkdirAll(config.TargetsDir, 0755); err != nil {
		return fmt.Errorf("create vm node exporter targets directory: %w", err)
	}

	matches, err := filepath.Glob(filepath.Join(config.TargetsDir, "fireactions-vm-*"+vmNodeExporterTargetSuffix))
	if err != nil {
		return fmt.Errorf("glob vm node exporter targets: %w", err)
	}

	for _, path := range matches {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale vm node exporter target %s: %w", path, err)
		}
	}

	return nil
}

func (p *Pool) writeVMNodeExporterTarget(machine *Machine) error {
	config := p.vmNodeExporterConfig
	if config == nil || !config.Enabled {
		return nil
	}

	addr := machine.GetAddr()
	if addr == "" {
		return fmt.Errorf("machine %s has no guest IP address", machine.Name)
	}

	if err := os.MkdirAll(config.TargetsDir, 0755); err != nil {
		return fmt.Errorf("create vm node exporter targets directory: %w", err)
	}

	labels := map[string]string{
		"organization": machine.Organization,
		"pool":         machine.Pool,
		"role":         "vm",
		"runner":       machine.Name,
	}
	if machine.WorkflowName != "" {
		labels["workflow"] = machine.WorkflowName
	}
	if machine.JobName != "" {
		labels["workflow_job"] = machine.JobName
	}

	target := prometheusFileSDTarget{
		Targets: []string{net.JoinHostPort(addr, strconv.Itoa(config.Port))},
		Labels:  labels,
	}

	data, err := yaml.Marshal([]prometheusFileSDTarget{target})
	if err != nil {
		return fmt.Errorf("marshal vm node exporter target: %w", err)
	}

	targetPath := vmNodeExporterTargetPath(config.TargetsDir, machine)
	tmpPath := targetPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write vm node exporter target: %w", err)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename vm node exporter target: %w", err)
	}

	return nil
}

func (p *Pool) removeVMNodeExporterTarget(machine *Machine) error {
	config := p.vmNodeExporterConfig
	if config == nil || !config.Enabled {
		return nil
	}

	err := os.Remove(vmNodeExporterTargetPath(config.TargetsDir, machine))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove vm node exporter target: %w", err)
	}

	return nil
}

func vmNodeExporterTargetPath(targetsDir string, machine *Machine) string {
	return filepath.Join(targetsDir, "fireactions-vm-"+sanitizeTargetFilename(machine.Pool+"-"+machine.Name)+vmNodeExporterTargetSuffix)
}

func sanitizeTargetFilename(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}

	name := strings.Trim(b.String(), "._-")
	if name == "" {
		return "machine"
	}

	return name
}
