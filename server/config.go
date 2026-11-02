package server

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

// Config is the configuration for the Client.
type Config struct {
	BindAddress      string            `yaml:"bind_address" validate:"required,hostname_port"`
	OnDemand         bool              `yaml:"on_demand" validate:""`
	Capacity         *CapacityConfig   `yaml:"capacity"`
	Containerd       *ContainerdConfig `yaml:"containerd" validate:"required"`
	Metrics          *MetricsConfig    `yaml:"metrics"`
	BasicAuthEnabled bool              `yaml:"basic_auth_enabled" validate:""`
	BasicAuthUsers   map[string]string `yaml:"basic_auth_users" validate:"required_if=basic_auth_enabled true"`
	GitHub           *GitHubConfig     `yaml:"github" validate:"required"`
	Pools            []*PoolConfig     `yaml:"pools" validate:"required,min=1"`
	LogLevel         string            `yaml:"log_level" validate:"required,oneof=debug info warn error fatal panic trace"`

	path string
}

type CapacityConfig struct {
	MemoryLimitMib int64 `yaml:"memory_limit_mib" validate:"min=0"`
	VCPULimit      int64 `yaml:"vcpu_limit" validate:"min=0"`
}

type ContainerdConfig struct {
	Address   string `yaml:"address" validate:"required"`
	Namespace string `yaml:"namespace" validate:"required"`
}

type MetricsConfig struct {
	Enabled        bool                  `yaml:"enabled" validate:""`
	Address        string                `yaml:"address" validate:"required_if=enabled true,hostname_port"`
	VMNodeExporter *VMNodeExporterConfig `yaml:"vm_node_exporter"`
}

type VMNodeExporterConfig struct {
	Enabled    bool   `yaml:"enabled" validate:""`
	Port       int    `yaml:"port"`
	TargetsDir string `yaml:"targets_dir"`
}

type GitHubConfig struct {
	AppPrivateKey string `yaml:"app_private_key" validate:"required"`
	AppID         int64  `yaml:"app_id" validate:"required"`
	WebhookSecret string `yaml:"webhook_secret" validate:""`
}

type RunnerConfig struct {
	Name            string   `yaml:"name" validate:"required"`
	ImagePullPolicy string   `yaml:"image_pull_policy" validate:"required,oneof=Always Never IfNotPresent"`
	Image           string   `yaml:"image" validate:"required"`
	Organization    string   `yaml:"organization" validate:"required"`
	GroupID         int64    `yaml:"group_id" validate:"required"`
	Labels          []string `yaml:"labels" validate:"required"`
}

type FirecrackerConfig struct {
	BinaryPath        string                   `yaml:"binary_path" `
	KernelImagePath   string                   `yaml:"kernel_image_path"`
	KernelArgs        string                   `yaml:"kernel_args"`
	RootFSInitialSize string                   `yaml:"rootfs_initial_size"`
	RootFSMaxSize     string                   `yaml:"rootfs_max_size"`
	CPUConfig         FirecrackerCPUConfig     `yaml:"cpu_config"`
	MachineConfig     FirecrackerMachineConfig `yaml:"machine_config"`
	Metadata          map[string]interface{}   `yaml:"metadata"`
}

type FirecrackerCPUConfig map[string]interface{}

type FirecrackerMachineConfig struct {
	VcpuCount  int64 `yaml:"vcpu_count" validate:"min=1"`
	MemSizeMib int64 `yaml:"mem_size_mib" validate:"min=1"`
}

// DefaultConfig creates a new Config with default values.
func DefaultConfig() *Config {
	c := &Config{
		BindAddress:      ":8080",
		OnDemand:         false,
		Capacity:         &CapacityConfig{},
		Containerd:       &ContainerdConfig{Address: "/run/containerd/containerd.sock", Namespace: "fireactions"},
		Metrics:          &MetricsConfig{Enabled: true, Address: ":8081", VMNodeExporter: &VMNodeExporterConfig{Port: 9100}},
		BasicAuthEnabled: false,
		BasicAuthUsers:   map[string]string{},
		GitHub:           &GitHubConfig{AppPrivateKey: "", AppID: 0},
		Pools:            []*PoolConfig{},
		LogLevel:         "debug",
	}

	return c
}

// NewConfigFromFile creates a new Config from a file.
func NewConfig(path string) (*Config, error) {
	c := DefaultConfig()
	c.path = path

	err := c.Load()
	if err != nil {
		return nil, err
	}

	err = c.Validate()
	if err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	return c, nil
}

// LoadFromFile loads the configuration from a file.
func (c *Config) Load() error {
	file, err := os.OpenFile(c.path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	defer func() {
		_ = file.Close()
	}()

	return yaml.NewDecoder(file).Decode(c)
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Capacity == nil {
		c.Capacity = &CapacityConfig{}
	}
	if c.Metrics == nil {
		c.Metrics = &MetricsConfig{}
	}
	if c.Metrics.VMNodeExporter == nil {
		c.Metrics.VMNodeExporter = &VMNodeExporterConfig{Port: 9100}
	}
	if c.Metrics.VMNodeExporter.Port == 0 {
		c.Metrics.VMNodeExporter.Port = 9100
	}

	if err := validator.New().Struct(c); err != nil {
		return err
	}

	if c.OnDemand {
		if c.GitHub == nil || c.GitHub.WebhookSecret == "" {
			return fmt.Errorf("github webhook_secret is required when on_demand is enabled")
		}
		if c.Metrics.Address == "" {
			return fmt.Errorf("metrics address is required when on_demand is enabled")
		}
	}

	if c.Metrics.VMNodeExporter.Enabled && c.Metrics.VMNodeExporter.TargetsDir == "" {
		return fmt.Errorf("metrics.vm_node_exporter.targets_dir is required when vm_node_exporter is enabled")
	}
	if c.Metrics.VMNodeExporter.Port < 1 || c.Metrics.VMNodeExporter.Port > 65535 {
		return fmt.Errorf("metrics.vm_node_exporter.port must be between 1 and 65535")
	}

	for i, pool := range c.Pools {
		if pool == nil {
			return fmt.Errorf("pool at index %d is required", i)
		}

		if pool.Firecracker == nil {
			return fmt.Errorf("pool %q firecracker config is required", pool.Name)
		}

		if pool.Firecracker.MachineConfig.MemSizeMib < 1 {
			return fmt.Errorf("pool %q mem_size_mib must be at least 1", pool.Name)
		}

		if pool.Firecracker.MachineConfig.VcpuCount < 1 {
			return fmt.Errorf("pool %q vcpu_count must be at least 1", pool.Name)
		}

		if len(pool.Firecracker.CPUConfig) > 0 {
			if _, err := json.Marshal(pool.Firecracker.CPUConfig); err != nil {
				return fmt.Errorf("pool %q cpu_config must be JSON-compatible: %w", pool.Name, err)
			}
		}

		if c.Capacity.MemoryLimitMib > 0 && pool.Firecracker.MachineConfig.MemSizeMib > c.Capacity.MemoryLimitMib {
			return fmt.Errorf("pool %q mem_size_mib %d exceeds capacity.memory_limit_mib %d",
				pool.Name, pool.Firecracker.MachineConfig.MemSizeMib, c.Capacity.MemoryLimitMib)
		}

		if c.Capacity.VCPULimit > 0 && pool.Firecracker.MachineConfig.VcpuCount > c.Capacity.VCPULimit {
			return fmt.Errorf("pool %q vcpu_count %d exceeds capacity.vcpu_limit %d",
				pool.Name, pool.Firecracker.MachineConfig.VcpuCount, c.Capacity.VCPULimit)
		}

		if c.OnDemand && !slices.Contains(pool.Runner.Labels, pool.Name) {
			return fmt.Errorf("pool %q runner.labels must include the pool name when on_demand is enabled", pool.Name)
		}
	}

	return nil
}
