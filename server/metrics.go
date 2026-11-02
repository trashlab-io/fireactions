package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "fireactions"
)

var (
	metricUp = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "server_up",
		Namespace: namespace,
		Help:      "Is the server up",
	})

	metricPoolsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "pools_total",
		Namespace: namespace,
		Help:      "Total number of pools",
	})

	metricPoolRunnersCurrent = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "pool_runners_current",
		Namespace: namespace,
		Help:      "Current number of running runners in a pool",
	}, []string{"pool", "organization"})

	metricPoolRunnersDesired = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "pool_runners_desired",
		Namespace: namespace,
		Help:      "Desired number of runners in a pool (replicas)",
	}, []string{"pool", "organization"})

	metricPoolRunnersPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "pool_runners_pending",
		Namespace: namespace,
		Help:      "Number of pending VM create/delete operations in a pool",
	}, []string{"pool", "organization"})

	metricPoolScaleRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "pool_scale_requests_total",
		Namespace: namespace,
		Help:      "Number of scale requests for a pool",
	}, []string{"pool", "organization"})

	metricScaleOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "scale_operations_total",
		Namespace: namespace,
		Help:      "Total number of scale operations",
	}, []string{"pool", "organization", "direction", "status"})

	metricScaleDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:      "scale_duration_seconds",
		Namespace: namespace,
		Help:      "Time taken to complete a scale operation",
		Buckets:   prometheus.DefBuckets,
	}, []string{"pool", "organization", "direction"})

	metricPoolStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "pool_status",
		Namespace: namespace,
		Help:      "Status of a pool. 0 is paused, 1 is active.",
	}, []string{"pool"})

	metricCapacityMemoryLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "capacity_memory_limit_mib",
		Namespace: namespace,
		Help:      "Configured global guest memory capacity limit in MiB. 0 means disabled.",
	})

	metricCapacityMemoryReserved = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "capacity_memory_reserved_mib",
		Namespace: namespace,
		Help:      "Currently reserved global guest memory capacity in MiB.",
	})

	metricCapacityVCPULimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "capacity_vcpu_limit",
		Namespace: namespace,
		Help:      "Configured global guest vCPU capacity limit. 0 means disabled.",
	})

	metricCapacityVCPUReserved = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "capacity_vcpu_reserved",
		Namespace: namespace,
		Help:      "Currently reserved global guest vCPU capacity.",
	})

	metricCapacityAdmissionBlocks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "capacity_admission_blocks_total",
		Namespace: namespace,
		Help:      "Number of VM admissions blocked by global capacity limits, or held for a pool that has waited longer (resource=waiting).",
	}, []string{"pool", "organization", "resource"})

	metricOnDemandJobsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "on_demand_jobs_active",
		Namespace: namespace,
		Help:      "Number of active queued or running workflow jobs targeting a pool.",
	}, []string{"pool", "organization"})

	metricOnDemandWebhookEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "on_demand_webhook_events_total",
		Namespace: namespace,
		Help:      "Number of processed GitHub on-demand webhook events.",
	}, []string{"action", "result"})

	metricOnDemandReconciliations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "on_demand_reconciliations_total",
		Namespace: namespace,
		Help:      "Number of on-demand reconciliation cycles.",
	}, []string{"result"})
)
