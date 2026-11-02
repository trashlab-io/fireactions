# Configuration

Fireactions is configured using a YAML file. The default configuration file is located at `/etc/fireactions/config.yaml`.

You can also specify a custom configuration file using the `--config` flag when starting the Fireactions server.

Example configuration file with all available options:

```yaml
---
#
# The address to listen on for HTTP requests.
#
# Default: :8080
#
bind_address: 0.0.0.0:8080

#
# Enable built-in on-demand scaling from GitHub workflow_job events.
#
# Default: false
#
on_demand: false

#
# Global host capacity limits across all pools.
#
capacity:
  #
  # Maximum total guest memory Fireactions may reserve across all running and in-flight VMs.
  # 0 disables the limit.
  #
  memory_limit_mib: 12288
  #
  # Maximum total guest vCPUs Fireactions may reserve across all running and in-flight VMs.
  # 0 disables the limit.
  #
  vcpu_limit: 8

#
# Metrics server configuration. This is used to expose Prometheus metrics on endpoint `/metrics`.
#
metrics:
  #
  # Enable Prometheus metrics.
  #
  enabled: true

  #
  # The address to listen on for HTTP requests.
  #
  address: 127.0.0.1:8081

  #
  # Optional Prometheus file_sd target generation for node_exporter processes
  # running inside active VMs.
  #
  vm_node_exporter:
    enabled: false
    port: 9100
    targets_dir: /var/lib/fireactions/prometheus-targets

#
# GitHub configuration.
#
github:
  #
  # The GitHub App private key. This is used to authenticate with GitHub.
  #
  # Default: ""
  #
  app_private_key: |
    -----BEGIN RSA PRIVATE KEY-----
  #
  # The GitHub App ID.
  #
  # Default: 0
  app_id: 12345
  #
  # GitHub webhook secret used to validate workflow_job webhooks.
  # Required when on_demand is enabled.
  #
  webhook_secret: your-webhook-secret

#
# Pools configuration.
#
pools:
  #
  # The name of the pool.
  #
- name: fireactions-2vcpu-2gb
  #
  # The number of replicas for the GitHub runners in the pool.
  #
  # Required: true
  #
  replicas: 5
  #
  # Shutdown the Firecracker VM when the runner exits.
  #
  # Required: false, Default: true
  #
  shutdown_on_exit: true
  #
  # GitHub runner configuration.
  #
  runner:
    #
    # The name of the GitHub runner. This is used to identify the runner in GitHub and is suffixed with a unique identifier.
    #
    # Required: true
    name: fireactions-2vcpu-2gb
    #
    # Container image to use for the Firecracker VM as the root device.
    #
    # Required: true
    #
    image: ghcr.io/hostinger/fireactions/runner:ubuntu-20.04-x64-2.310.2
    #
    # The pull policy for the container image. Can be one of: Always, IfNotPresent, Never.
    #
    # Required: true
    image_pull_policy: IfNotPresent
    #
    # GitHub runner group ID. 1 is the default group.
    #
    # Required: true
    group_id: 1
    #
    # Organization name.
    #
    # Required: true
    #
    organization: hostinger
    #
    # Labels to apply to the GitHub runner.
    #
    # Required: true
    #
    labels:
    - self-hosted
    - fireactions-2vcpu-2gb
    - fireactions
    #
    # When on_demand is enabled, labels must include the pool name so Fireactions
    # can map queued jobs to exactly one pool.
    #
  #
  # Firecracker configuration.
  #
  firecracker:
    #
    # The path to the Firecracker binary.
    #
    # Default: firecracker
    #
    binary_path: firecracker
    #
    # The path to the kernel image.
    #
    # Required: true
    #
    kernel_image_path: /var/lib/fireactions/vmlinux
    #
    # Kernel command line arguments.
    #
    # Default: "console=ttyS0 noapic reboot=k panic=1 pci=off nomodules rw"
    #
    kernel_args: "console=ttyS0 noapic reboot=k panic=1 pci=off nomodules rw"
    #
    # Optional Firecracker CPU configuration passed to the pre-boot /cpu-config API.
    # This can expose or mask CPUID/MSR/KVM capability bits for specialized guests.
    #
    # Default: {}
    #
    cpu_config:
      cpuid_modifiers:
      - leaf: "0x80000001"
        subleaf: "0x0"
        flags: 0
        modifiers:
        - register: ecx
          bitmap: "0bxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1xx"
    #
    # Firecracker machine configuration.
    #
    # Required: true
    #
    machine_config:
      #
      # The amount of memory in MiB.
      #
      # Required: true
      #
      mem_size_mib: 2048
      #
      # The number of vCPUs.
      #
      # Required: true
      #
      vcpu_count: 2
    #
    # Metadata to pass to the Firecracker VM via MMDS.
    #
    # Default: {}
    #
    metadata:
      example1: value1
      example2: value2

#
# Log level. Can be one of: debug, info, warn, error, fatal, panic, trace.
#
# Default: info
#
log_level: debug
```
