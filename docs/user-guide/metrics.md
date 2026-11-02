# Metrics

Fireactions provides Prometheus metrics for monitoring.

The metrics can be enabled by setting the `metrics.enabled` configuration option to `true`. The metrics are exposed on the `/metrics` endpoint on the address and port specified in the `metrics.address` and `metrics.port` configuration options.

Fireactions can also maintain Prometheus `file_sd` target files for node_exporter processes running inside active VMs. Enable `metrics.vm_node_exporter`, point `targets_dir` at a directory scraped by Prometheus, and run node_exporter inside the guest image on the configured port.

## Metrics

The following metrics are available, excluding the default Prometheus metrics:

| Metric Name                                  | Type      | Description                                               | Labels                                           |
|----------------------------------------------|-----------|-----------------------------------------------------------|--------------------------------------------------|
| `fireactions_server_up`                      | Gauge     | Whether the server is up (1) or down (0)                  | None                                             |
| `fireactions_pools_total`                    | Gauge     | Total number of pools                                     | None                                             |
| `fireactions_pool_runners_current`           | Gauge     | Current number of running runners in a pool               | `pool`, `organization`                           |
| `fireactions_pool_runners_desired`           | Gauge     | Desired number of runners in a pool (replicas)            | `pool`, `organization`                           |
| `fireactions_pool_status`                    | Gauge     | Status of a pool (0 = paused, 1 = active)                 | `pool`                                           |
| `fireactions_pool_scale_requests_total`      | Counter   | Number of scale API requests for a pool                   | `pool`                                           |
| `fireactions_scale_operations_total`         | Counter   | Total number of individual scale operations               | `pool`, `organization`, `direction`, `status`    |
| `fireactions_scale_duration_seconds`         | Histogram | Time taken to complete a scale operation                  | `pool`, `organization`, `direction`              |
| `fireactions_capacity_memory_limit_mib`      | Gauge     | Configured global guest memory limit in MiB               | None                                             |
| `fireactions_capacity_memory_reserved_mib`   | Gauge     | Reserved global guest memory in MiB                       | None                                             |
| `fireactions_capacity_vcpu_limit`            | Gauge     | Configured global guest vCPU limit                        | None                                             |
| `fireactions_capacity_vcpu_reserved`         | Gauge     | Reserved global guest vCPU capacity                       | None                                             |
| `fireactions_capacity_admission_blocks_total`| Counter   | VM admissions blocked by global capacity                  | `pool`, `organization`, `resource`               |
| `fireactions_vm_cpu_seconds_total`           | Counter   | Cumulative host-observed CPU seconds for an active VM      | `pool`, `organization`, `runner`                 |
| `fireactions_vm_memory_rss_bytes`            | Gauge     | Current host-observed Firecracker process RSS bytes        | `pool`, `organization`, `runner`                 |
| `fireactions_vm_configured_memory_bytes`     | Gauge     | Configured guest memory bytes for an active VM             | `pool`, `organization`, `runner`                 |
| `fireactions_vm_configured_vcpus`            | Gauge     | Configured guest vCPU count for an active VM               | `pool`, `organization`, `runner`                 |


Example Grafana dashboard for vizualisation of Fireactions metrics:

![Grafana Dashboard](../img/grafana-dashboard.png)
