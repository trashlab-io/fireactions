package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	units "github.com/docker/go-units"
	"github.com/firecracker-microvm/firecracker-go-sdk"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/models"
	"github.com/hostinger/fireactions/helper/deepcopy"
	"github.com/hostinger/fireactions/helper/github"
	"github.com/hostinger/fireactions/helper/stringid"
	"github.com/opencontainers/image-spec/identity"
	"github.com/rs/zerolog"
	"github.com/sirupsen/logrus"

	githubv63 "github.com/google/go-github/v63/github"
)

const (
	defaultSnapshotter        = "devmapper"
	scaleDownStateTimeout     = 2 * time.Second
	scaleDownRunnerAPITimeout = 10 * time.Second
	configureCPUHandlerName   = "fcinit.ConfigureCPU"
)

var errBusyRunner = errors.New("runner is busy")

// Pool represents a pool of Firecracker VMs that are used to run GitHub Actions jobs.
type Pool struct {
	config                *PoolConfig
	vmNodeExporterConfig  *VMNodeExporterConfig
	containerd            *containerd.Client
	github                *github.Client
	imageManager          *imageManager
	capacity              *CapacityManager
	pendingCreates        atomic.Int32
	pendingDeletes        atomic.Int32
	machinesMu            *sync.Mutex
	machines              map[string]*Machine
	installationID        atomic.Int64
	logger                *zerolog.Logger
	baseReplicas          atomic.Int32
	demandReplicas        atomic.Int32
	desiredReplicas       atomic.Int32
	isActive              bool
	onDemandRunnersMu     sync.Mutex
	onDemandRunners       []demandRunnerMetadata
	scaleTrigger          chan struct{}
	stopCh                chan struct{}
	doneCh                chan struct{}
	cleanupWg             sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	nextCID               *atomic.Uint32
	l                     *sync.Mutex
	capacityBlockedReason CapacityBlockReason
	createMachineFn       func(context.Context, *CapacityReservation) error
	removeRunnerFn        func(context.Context, string, int64) error
	shutdownWaitTimeout   time.Duration
}

// PoolConfig represents the configuration of a Pool.
type PoolConfig struct {
	Name           string             `yaml:"name" validate:"required"`
	ShutdownOnExit *bool              `yaml:"shutdown_on_exit"`
	Replicas       int                `yaml:"replicas" validate:"min=0"`
	Runner         *RunnerConfig      `yaml:"runner" validate:"required"`
	Firecracker    *FirecrackerConfig `yaml:"firecracker" validate:"required"`
}

// UnmarshalYAML implements custom unmarshaling to set defaults.
func (p *PoolConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type poolConfigAlias PoolConfig
	defaults := poolConfigAlias{
		ShutdownOnExit: func() *bool { b := true; return &b }(),
	}

	if err := unmarshal(&defaults); err != nil {
		return err
	}

	*p = PoolConfig(defaults)
	return nil
}

// NewPool creates a new Pool.
func NewPool(logger *zerolog.Logger, config *PoolConfig, github *github.Client, imageManager *imageManager, containerdClient *containerd.Client, nextCID *atomic.Uint32, capacity *CapacityManager, vmNodeExporterConfig *VMNodeExporterConfig) (*Pool, error) {
	l := logger.With().Str("pool", config.Name).Logger()

	ctx, cancel := context.WithCancel(context.Background())

	if capacity == nil {
		capacity = NewCapacityManager(nil)
	}

	p := &Pool{
		config:               config,
		vmNodeExporterConfig: vmNodeExporterConfig,
		l:                    &sync.Mutex{},
		machinesMu:           &sync.Mutex{},
		machines:             make(map[string]*Machine),
		isActive:             true,
		containerd:           containerdClient,
		github:               github,
		imageManager:         imageManager,
		capacity:             capacity,
		logger:               &l,
		scaleTrigger:         make(chan struct{}, 1),
		stopCh:               make(chan struct{}, 1),
		doneCh:               make(chan struct{}),
		ctx:                  ctx,
		cancel:               cancel,
		nextCID:              nextCID,
		shutdownWaitTimeout:  30 * time.Second,
	}

	p.baseReplicas.Store(int32(config.Replicas))
	p.desiredReplicas.Store(int32(config.Replicas))

	if _, err := os.Stat(p.GetDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(p.GetDir(), 0755); err != nil {
			return nil, fmt.Errorf("creating pool directory: %w", err)
		}

		p.logger.Debug().Msgf("Pool directory created at %s", p.GetDir())
	}

	metricPoolRunnersCurrent.
		WithLabelValues(p.config.Name, p.config.Runner.Organization).Set(float64(p.GetCurrentSize()))
	metricPoolRunnersDesired.
		WithLabelValues(p.config.Name, p.config.Runner.Organization).Set(float64(p.GetDesiredReplicas()))
	metricPoolStatus.
		WithLabelValues(p.config.Name).Set(1)

	metricPoolsTotal.Inc()

	return p, nil
}

// Run starts the pool. Starting the pool will start the scaling process.
func (p *Pool) Run() {
	defer close(p.doneCh) // Signal that Run() has exited

	// Trigger initial scale
	p.TriggerScale()

	for {
		select {
		case <-p.scaleTrigger:
		case <-time.After(2 * time.Second):
		case <-p.stopCh:
			return
		case <-p.ctx.Done():
			return
		}

		// Check if we should stop before scaling (non-blocking check)
		select {
		case <-p.ctx.Done():
			return
		case <-p.stopCh:
			return
		default:
		}

		curSize := p.GetCurrentSize()
		desiredReplicas := p.GetDesiredReplicas()
		pendingCreates := int(p.pendingCreates.Load())
		pendingDeletes := int(p.pendingDeletes.Load())
		netPending := pendingCreates - pendingDeletes
		metricPoolRunnersCurrent.
			WithLabelValues(p.config.Name, p.config.Runner.Organization).Set(float64(curSize))
		metricPoolRunnersDesired.
			WithLabelValues(p.config.Name, p.config.Runner.Organization).Set(float64(desiredReplicas))
		metricPoolRunnersPending.
			WithLabelValues(p.config.Name, p.config.Runner.Organization).Set(float64(netPending))

		if !p.isActive {
			p.logger.Debug().Msgf("Pool %s is paused, skipping scaling", p.config.Name)
			continue
		}

		// Scale to desired replicas
		if err := p.Scale(p.ctx, desiredReplicas); err != nil {
			// Don't log errors if context was cancelled (pool is stopping)
			if p.ctx.Err() == nil {
				p.logger.Error().Err(err).Msg("Failed to scale pool")
			}
		}
	}
}

// Stop stops the pool. Stopping the pool will stop all the VMs in the pool.
func (p *Pool) Stop() {
	p.logger.Debug().Msgf("Stopping pool %s", p.config.Name)
	p.cancel()

	// Signal the Start() loop to exit (non-blocking)
	select {
	case p.stopCh <- struct{}{}:
	default:
		// Channel already has a value or Start() already exited
	}

	// Wait for Run() loop to exit cleanly with a timeout
	select {
	case <-p.doneCh:
	case <-time.After(5 * time.Second):
		p.logger.Warn().Msg("Timeout waiting for Run() to exit")
	}

	p.logger.Debug().Msgf("Stopping %d machines in pool %s", len(p.machines), p.config.Name)

	p.machinesMu.Lock()
	machines := make([]*Machine, 0, len(p.machines))
	for _, machine := range p.machines {
		machines = append(machines, machine)
	}
	p.machinesMu.Unlock()

	// Stop all machines - cleanup goroutines will handle the rest
	for _, machine := range machines {
		runnerName := machine.Name

		err := machine.Stop()
		if err != nil {
			p.logger.Error().Err(err).Msgf("Failed to stop Firecracker VM %s", runnerName)
		}

		p.logger.Debug().Msgf("Stopped Firecracker VM %s", runnerName)
	}

	cleanupDone := make(chan struct{})
	go func() {
		p.cleanupWg.Wait()
		close(cleanupDone)
	}()

	select {
	case <-cleanupDone:
	case <-time.After(35 * time.Second):
		p.logger.Warn().Msg("Timeout waiting for cleanup goroutines to finish")
	}

	p.logger.Debug().Msgf("Pool %s stopped", p.config.Name)
}

// GetDir returns the directory where the pool sockets and logs are stored.
func (p *Pool) GetDir() string {
	return fmt.Sprintf("/var/lib/fireactions/pools/%s", p.config.Name)
}

// Scale scales the pool to the desired size.
func (p *Pool) Scale(ctx context.Context, desiredReplicas int) error {
	p.l.Lock()
	defer p.l.Unlock()

	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
	}

	curSize := p.GetCurrentSize()
	pendingCreates := int(p.pendingCreates.Load())
	pendingDeletes := int(p.pendingDeletes.Load())

	// GetCurrentSize excludes draining machines, so pendingDeletes must not be
	// subtracted here or the same in-flight delete will be counted twice.
	effectiveSize := curSize + pendingCreates
	delta := desiredReplicas - effectiveSize

	if delta == 0 {
		p.capacity.Forget(p.config.Name)
		p.clearCapacityBlocked(false)
		return nil
	}

	if delta > 0 {
		result := p.scaleUp(
			ctx, delta, desiredReplicas, curSize, pendingCreates, pendingDeletes)
		p.updateCapacityBlockedState(result, desiredReplicas, curSize)
	} else {
		p.capacity.Forget(p.config.Name)
		p.clearCapacityBlocked(false)
		p.scaleDown(
			ctx, -delta, desiredReplicas, curSize, pendingCreates, pendingDeletes)
	}

	return nil
}

type scaleUpResult struct {
	granted int
	blocked int
	reason  CapacityBlockReason
}

func (p *Pool) scaleUp(ctx context.Context, count, desiredReplicas, curSize, pendingCreates, pendingRemovals int) scaleUpResult {
	p.logger.Debug().Msgf("Scaling up by %d VMs (target: %d, current: %d, pending creates: %d, pending removals: %d)",
		count, desiredReplicas, curSize, pendingCreates, pendingRemovals)

	memPerVM := p.config.Firecracker.MachineConfig.MemSizeMib
	vcpuPerVM := p.config.Firecracker.MachineConfig.VcpuCount
	reservations, reason := p.capacity.ReserveFor(p.config.Name, count, memPerVM, vcpuPerVM)
	blocked := count - len(reservations)

	if blocked > 0 {
		metricScaleOperations.WithLabelValues(p.config.Name, p.config.Runner.Organization, "up", "blocked").Add(float64(blocked))
		switch reason {
		case CapacityBlockReasonMemory:
			metricCapacityAdmissionBlocks.WithLabelValues(p.config.Name, p.config.Runner.Organization, "memory").Add(float64(blocked))
		case CapacityBlockReasonVCPU:
			metricCapacityAdmissionBlocks.WithLabelValues(p.config.Name, p.config.Runner.Organization, "vcpu").Add(float64(blocked))
		case CapacityBlockReasonBoth:
			metricCapacityAdmissionBlocks.WithLabelValues(p.config.Name, p.config.Runner.Organization, "memory").Add(float64(blocked))
			metricCapacityAdmissionBlocks.WithLabelValues(p.config.Name, p.config.Runner.Organization, "vcpu").Add(float64(blocked))
		case CapacityBlockReasonWaiting:
			metricCapacityAdmissionBlocks.WithLabelValues(p.config.Name, p.config.Runner.Organization, "waiting").Add(float64(blocked))
		}
	}

	for _, reservation := range reservations {
		p.pendingCreates.Add(1)

		go func(reservation *CapacityReservation) {
			defer p.pendingCreates.Add(-1)

			select {
			case <-p.ctx.Done():
				reservation.Release()
				return
			default:
			}

			start := time.Now()
			if err := p.runCreateMachine(ctx, reservation); err != nil {
				// A failed create never owns a machine, so give the capacity back.
				// Release is idempotent, so creates that already released are fine.
				reservation.Release()
				metricScaleOperations.WithLabelValues(p.config.Name, p.config.Runner.Organization, "up", "failure").Inc()
				p.logger.Error().Err(err).Msg("Failed to create machine")
				return
			}

			duration := time.Since(start).Seconds()
			metricScaleOperations.WithLabelValues(p.config.Name, p.config.Runner.Organization, "up", "success").Inc()
			metricScaleDuration.WithLabelValues(p.config.Name, p.config.Runner.Organization, "up").Observe(duration)
		}(reservation)
	}

	return scaleUpResult{
		granted: len(reservations),
		blocked: blocked,
		reason:  reason,
	}
}

func (p *Pool) scaleDown(ctx context.Context, count, desiredReplicas, curSize, pendingCreates, pendingDeletes int) {
	if count > curSize {
		count = curSize
	}

	p.logger.Debug().Msgf("Scaling down by %d VMs (target: %d, current: %d, pending creates: %d, pending deletes: %d)",
		count, desiredReplicas, curSize, pendingCreates, pendingDeletes)

	for i := 0; i < count; i++ {
		targetMachine, targetName, ok := p.beginDeleteMachine(ctx)
		if !ok {
			return
		}

		p.pendingDeletes.Add(1)

		go func(targetMachine *Machine, targetName string) {
			defer p.pendingDeletes.Add(-1)

			select {
			case <-p.ctx.Done():
				return
			default:
			}

			start := time.Now()
			if err := p.stopSelectedMachine(targetMachine, targetName); err != nil {
				if errors.Is(err, errBusyRunner) {
					p.logger.Info().Str("machine", targetName).Msg("Skipping scale down for busy runner")
					return
				}
				metricScaleOperations.WithLabelValues(p.config.Name, p.config.Runner.Organization, "down", "failure").Inc()
				p.logger.Error().Err(err).Msg("Failed to delete machine")
				return
			}

			duration := time.Since(start).Seconds()
			metricScaleOperations.WithLabelValues(p.config.Name, p.config.Runner.Organization, "down", "success").Inc()
			metricScaleDuration.WithLabelValues(p.config.Name, p.config.Runner.Organization, "down").Observe(duration)
		}(targetMachine, targetName)
	}
}

func (p *Pool) updateCapacityBlockedState(result scaleUpResult, desiredReplicas, curSize int) {
	if result.blocked == 0 {
		p.clearCapacityBlocked(true)
		return
	}

	if p.capacityBlockedReason == result.reason {
		return
	}

	p.capacityBlockedReason = result.reason
	snapshot := p.capacity.Snapshot()

	p.logger.Warn().
		Int("desired_replicas", desiredReplicas).
		Int("current_replicas", curSize).
		Int64("vm_mem_size_mib", p.config.Firecracker.MachineConfig.MemSizeMib).
		Int64("vm_vcpu_count", p.config.Firecracker.MachineConfig.VcpuCount).
		Int("granted", result.granted).
		Int("blocked", result.blocked).
		Int64("reserved_memory_mib", snapshot.MemoryReservedMib).
		Int64("memory_limit_mib", snapshot.MemoryLimitMib).
		Int64("reserved_vcpu", snapshot.VCPUReserved).
		Int64("vcpu_limit", snapshot.VCPULimit).
		Str("blocked_by", string(result.reason)).
		Msg("Global capacity blocked VM admission")
}

func (p *Pool) clearCapacityBlocked(logResume bool) {
	if p.capacityBlockedReason == CapacityBlockReasonNone {
		return
	}

	previous := p.capacityBlockedReason
	p.capacityBlockedReason = CapacityBlockReasonNone
	if !logResume {
		return
	}

	snapshot := p.capacity.Snapshot()
	p.logger.Info().
		Str("previous_blocked_by", string(previous)).
		Int64("reserved_memory_mib", snapshot.MemoryReservedMib).
		Int64("reserved_vcpu", snapshot.VCPUReserved).
		Msg("Global capacity admission resumed")
}

// Pause pauses the pool. Pausing the pool will prevent the pool from scaling.
func (p *Pool) Pause() {
	if !p.isActive {
		return
	}

	p.logger.Debug().Msgf("Pool %s state changed to paused", p.config.Name)
	p.isActive = false
}

// Resume resumes the pool. Resuming the pool will allow the pool to scale.
func (p *Pool) Resume() {
	if p.isActive {
		return
	}

	p.logger.Debug().Msgf("Pool %s state changed to active", p.config.Name)
	p.isActive = true
	p.TriggerScale()
}

// SetReplicas updates the configured warm replica count for the pool in a thread-safe manner.
func (p *Pool) SetReplicas(replicas int) {
	p.baseReplicas.Store(int32(replicas))
	p.recomputeDesiredReplicas()
}

// SetDemandReplicas updates the on-demand replica target for the pool.
func (p *Pool) SetDemandReplicas(replicas int) {
	p.SetDemandReplicasWithRunnerMetadata(replicas, nil)
}

// SetDemandReplicasWithRunnerMetadata updates the on-demand replica target and
// metadata used to name new VMs created for queued workflow jobs.
func (p *Pool) SetDemandReplicasWithRunnerMetadata(replicas int, runners []demandRunnerMetadata) {
	p.onDemandRunnersMu.Lock()
	p.onDemandRunners = append([]demandRunnerMetadata(nil), runners...)
	p.onDemandRunnersMu.Unlock()

	p.demandReplicas.Store(int32(replicas))
	p.recomputeDesiredReplicas()
}

func (p *Pool) recomputeDesiredReplicas() {
	baseReplicas := p.GetBaseReplicas()
	demandReplicas := int(p.demandReplicas.Load())
	desiredReplicas := max(baseReplicas, demandReplicas)
	p.desiredReplicas.Store(int32(desiredReplicas))
	p.TriggerScale()
}

// TriggerScale sends a non-blocking notification to trigger scaling.
func (p *Pool) TriggerScale() {
	select {
	case p.scaleTrigger <- struct{}{}:
	default:
	}
}

// GetBaseReplicas returns the configured warm replica count for the pool.
func (p *Pool) GetBaseReplicas() int {
	return int(p.baseReplicas.Load())
}

// GetDesiredReplicas returns the effective desired replica count for the pool in a thread-safe manner.
func (p *Pool) GetDesiredReplicas() int {
	return int(p.desiredReplicas.Load())
}

// GetReplicas returns the effective desired replica count for the pool in a thread-safe manner.
func (p *Pool) GetReplicas() int {
	return p.GetDesiredReplicas()
}

// GetCurrentSize returns the current size of the pool.
func (p *Pool) GetCurrentSize() int {
	p.machinesMu.Lock()
	defer p.machinesMu.Unlock()

	size := 0
	for _, machine := range p.machines {
		// Machines marked as stopping are already draining out of the pool and
		// should not contribute to the effective pool size during reconciliation.
		if machine.stopping {
			continue
		}
		size++
	}

	return size
}

func (p *Pool) ListMachines(ctx context.Context) ([]*Machine, error) {
	p.machinesMu.Lock()
	defer p.machinesMu.Unlock()

	machines := make([]*Machine, 0, len(p.machines))
	for _, machine := range p.machines {
		machines = append(machines, machine)
	}

	return machines, nil
}

func (p *Pool) GetMachine(name string) (*Machine, error) {
	p.machinesMu.Lock()
	defer p.machinesMu.Unlock()

	machine, ok := p.machines[name]
	if !ok {
		return nil, fmt.Errorf("machine not found: %s", name)
	}

	return machine, nil
}

func (p *Pool) runCreateMachine(ctx context.Context, reservation *CapacityReservation) error {
	if p.createMachineFn != nil {
		return p.createMachineFn(ctx, reservation)
	}

	return p.createMachine(ctx, reservation)
}

func (p *Pool) createMachine(ctx context.Context, reservation *CapacityReservation) error {
	image, err := p.imageManager.ensureImage(
		ctx,
		p.config.Runner.Image,
		p.config.Runner.ImagePullPolicy,
	)
	if err != nil {
		return fmt.Errorf("ensuring image: %w", err)
	}

	demandRunner := p.nextDemandRunnerMetadata()
	runnerName := p.runnerName(demandRunner.Prefix)

	leaseCtx, leaseCtxCancel, err := p.containerd.WithLease(ctx,
		leases.WithID(fmt.Sprintf("fireactions/pools/%s/%s", p.config.Name, runnerName)))
	if err != nil {
		return fmt.Errorf("containerd: creating lease: %w", err)
	}

	// Track if we successfully created the machine to determine cleanup responsibility
	var machineCreated bool
	defer func() {
		if !machineCreated {
			if reservation != nil {
				reservation.Release()
			}
			// Clean up lease if machine creation failed
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_ = leaseCtxCancel(cleanupCtx)
		}
	}()

	snapshotMounts, err := p.createSnapshot(leaseCtx, image, runnerName)
	if err != nil {
		return fmt.Errorf("containerd: creating snapshot: %w", err)
	}
	if err := p.prepareRootFSSize(ctx, snapshotMounts[0].Source, runnerName); err != nil {
		return fmt.Errorf("rootfs sizing: %w", err)
	}

	machineLogFile, err := os.Create(filepath.Join(p.GetDir(), fmt.Sprintf("%s.log", runnerName)))
	if err != nil {
		return fmt.Errorf("creating log file: %w", err)
	}
	defer machineLogFile.Close()

	machineCmd := firecracker.VMCommandBuilder{}.
		WithSocketPath(filepath.Join(p.GetDir(), fmt.Sprintf("%s.sock", runnerName))).
		WithStderr(machineLogFile).
		WithStdout(machineLogFile).
		WithBin(p.config.Firecracker.BinaryPath).
		Build(ctx)

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetOutput(io.Discard)

	vsockPath := filepath.Join(p.GetDir(), fmt.Sprintf("%s.vsock", runnerName))
	vsockCID := p.nextCID.Add(1)

	fcMachine, err := firecracker.NewMachine(ctx, firecracker.Config{
		VMID:            runnerName,
		SocketPath:      filepath.Join(p.GetDir(), fmt.Sprintf("%s.sock", runnerName)),
		KernelImagePath: p.config.Firecracker.KernelImagePath,
		KernelArgs:      p.config.Firecracker.KernelArgs,
		MachineCfg: models.MachineConfiguration{
			VcpuCount:  &p.config.Firecracker.MachineConfig.VcpuCount,
			MemSizeMib: &p.config.Firecracker.MachineConfig.MemSizeMib,
		},
		Drives: []models.Drive{{
			DriveID:      firecracker.String("rootfs"),
			PathOnHost:   &snapshotMounts[0].Source,
			IsRootDevice: firecracker.Bool(true),
			IsReadOnly:   firecracker.Bool(false),
		}},
		NetworkInterfaces: []firecracker.NetworkInterface{{
			AllowMMDS:        true,
			CNIConfiguration: &firecracker.CNIConfiguration{NetworkName: "fireactions", IfName: "eth0", ConfDir: "/etc/cni/net.d", BinPath: []string{"/opt/cni/bin"}},
		}},
		VsockDevices:   []firecracker.VsockDevice{{Path: vsockPath, CID: vsockCID}},
		MmdsAddress:    net.IPv4(169, 254, 169, 254),
		MmdsVersion:    firecracker.MMDSv2,
		ForwardSignals: []os.Signal{},
		LogPath:        filepath.Join(p.GetDir(), fmt.Sprintf("%s.firecracker.log", runnerName)),
		LogLevel:       "Debug",
	}, firecracker.WithProcessRunner(machineCmd), firecracker.WithLogger(logrus.NewEntry(logger)))
	if err != nil {
		return fmt.Errorf("firecracker: creating machine: %w", err)
	}
	if len(p.config.Firecracker.CPUConfig) > 0 {
		fcMachine.Handlers.FcInit = fcMachine.Handlers.FcInit.AppendAfter(
			firecracker.BootstrapLoggingHandlerName,
			firecracker.Handler{
				Name: configureCPUHandlerName,
				Fn: func(ctx context.Context, m *firecracker.Machine) error {
					return putFirecrackerCPUConfig(ctx, m.Cfg.SocketPath, p.config.Firecracker.CPUConfig)
				},
			},
		)
	}

	installationID := p.installationID.Load()
	if installationID == 0 {
		installation, _, err := p.github.Apps.FindOrganizationInstallation(ctx, p.config.Runner.Organization)
		if err != nil {
			return fmt.Errorf("github: %w", err)
		}
		installationID = installation.GetID()

		p.installationID.Store(installationID)
	}

	client := p.github.Installation(installationID)
	jitConfig, _, err := client.Actions.GenerateOrgJITConfig(ctx, p.config.Runner.Organization, &githubv63.GenerateJITConfigRequest{
		Name:          runnerName,
		RunnerGroupID: p.config.Runner.GroupID,
		Labels:        p.config.Runner.Labels,
	})
	if err != nil {
		return fmt.Errorf("github: %w", err)
	}

	metadata := map[string]interface{}{"latest": map[string]interface{}{"meta-data": deepcopy.Map(p.config.Firecracker.Metadata)}}
	metadata["latest"].(map[string]interface{})["meta-data"].(map[string]interface{})["fireactions"] = map[string]interface{}{
		"runner_id":         runnerName,
		"runner_jit_config": jitConfig.GetEncodedJITConfig(),
		"hostname":          runnerName,
		"shutdown_on_exit":  *p.config.ShutdownOnExit,
	}

	fcMachine.Handlers.FcInit = fcMachine.Handlers.FcInit.Append(firecracker.NewSetMetadataHandler(metadata))

	vmmCtx, vmmCancel := context.WithCancel(p.ctx)
	if err := fcMachine.Start(vmmCtx); err != nil {
		vmmCancel()
		return fmt.Errorf("firecracker: starting machine: %w", err)
	}

	processID, err := fcMachine.PID()
	if err != nil {
		p.logger.Warn().Err(err).Msgf("Failed to resolve Firecracker process ID for VM %s; host VM metrics will be unavailable", runnerName)
	}

	// Mark machine as successfully created
	machineCreated = true

	p.logger.Info().Msgf("Successfully created Firecracker VM %s", runnerName)

	machine := &Machine{
		Machine:      fcMachine,
		Name:         jitConfig.GetRunner().GetName(),
		RunnerID:     jitConfig.GetRunner().GetID(),
		Pool:         p.config.Name,
		Organization: p.config.Runner.Organization,
		ProcessID:    processID,
		CreatedAt:    time.Now().UTC(),
		MemoryMib:    p.config.Firecracker.MachineConfig.MemSizeMib,
		VCPUCount:    p.config.Firecracker.MachineConfig.VcpuCount,
		WorkflowName: demandRunner.WorkflowName,
		JobName:      demandRunner.JobName,
		Reservation:  reservation,
		vsockCID:     vsockCID,
		vsockPath:    vsockPath,
		leaseCancel:  leaseCtxCancel,
		vmmCtx:       vmmCtx,
		vmmCancel:    vmmCancel,
	}

	p.trackMachine(machine)

	return nil
}

func (p *Pool) trackMachine(machine *Machine) {
	if machine.Organization == "" {
		machine.Organization = p.config.Runner.Organization
	}

	p.machinesMu.Lock()
	p.machines[machine.Name] = machine
	p.machinesMu.Unlock()

	metricVMHostProcess.track(machine)
	if err := p.writeVMNodeExporterTarget(machine); err != nil {
		p.logger.Warn().Err(err).Str("machine", machine.Name).Msg("Failed to write VM node exporter target")
	}

	p.cleanupWg.Add(1)
	go func() {
		defer p.cleanupWg.Done()
		defer metricVMHostProcess.untrack(machine)
		defer func() {
			if err := p.removeVMNodeExporterTarget(machine); err != nil {
				p.logger.Warn().Err(err).Str("machine", machine.Name).Msg("Failed to remove VM node exporter target")
			}
		}()

		waitDone := make(chan error, 1)
		go func() {
			waitDone <- machine.WaitForExit(context.Background())
		}()

		select {
		case <-waitDone:
		case <-p.ctx.Done():
			select {
			case <-waitDone:
			case <-time.After(p.shutdownWaitTimeout):
				p.logger.Warn().Msgf("Timeout waiting for machine %s to exit during pool shutdown", machine.Name)
			}
		}

		p.machinesMu.Lock()
		current, exists := p.machines[machine.Name]
		if exists && current == machine {
			delete(p.machines, machine.Name)
		}
		p.machinesMu.Unlock()

		if machine.Reservation != nil {
			machine.Reservation.Release()
		}

		if machine.vmmCancel != nil {
			machine.vmmCancel()
		}

		ctx, cancel := context.WithTimeout(context.Background(), scaleDownRunnerAPITimeout)
		if err := p.removeGitHubRunner(ctx, machine.Name, machine.RunnerID); err != nil && !errors.Is(err, errBusyRunner) {
			p.logger.Error().Err(err).Msgf("Failed to delete GitHub runner %s (ID: %d)", machine.Name, machine.RunnerID)
		}
		cancel()

		if machine.leaseCancel != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := machine.leaseCancel(ctx)
			if err != nil && !errdefs.IsNotFound(err) {
				p.logger.Error().Err(err).Msgf("Failed to remove Containerd lease for Firecracker VM %s", machine.Name)
			}
		}

		p.logger.Info().Msgf("Successfully cleaned up exited Firecracker VM %s", machine.Name)
	}()
}

func (p *Pool) nextDemandRunnerMetadata() demandRunnerMetadata {
	p.onDemandRunnersMu.Lock()
	defer p.onDemandRunnersMu.Unlock()

	if len(p.onDemandRunners) == 0 {
		return demandRunnerMetadata{}
	}

	metadata := p.onDemandRunners[0]
	copy(p.onDemandRunners, p.onDemandRunners[1:])
	p.onDemandRunners = p.onDemandRunners[:len(p.onDemandRunners)-1]
	return metadata
}

func (p *Pool) runnerName(prefix string) string {
	if prefix == "" {
		prefix = p.config.Runner.Name
	}

	suffix := stringid.New()
	maxPrefixLen := 63 - len(suffix) - 1
	if maxPrefixLen < 1 {
		return suffix
	}
	if len(prefix) > maxPrefixLen {
		prefix = strings.Trim(prefix[:maxPrefixLen], "-")
	}
	if prefix == "" {
		prefix = p.config.Runner.Name
	}

	return fmt.Sprintf("%s-%s", prefix, suffix)
}

func putFirecrackerCPUConfig(ctx context.Context, socketPath string, cpuConfig FirecrackerCPUConfig) error {
	body, err := json.Marshal(cpuConfig)
	if err != nil {
		return fmt.Errorf("marshal cpu_config: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://unix/cpu-config", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create cpu-config request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("put cpu-config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("put cpu-config returned %s: %s", resp.Status, bytes.TrimSpace(responseBody))
	}

	return nil
}

func (p *Pool) beginDeleteMachine(ctx context.Context) (*Machine, string, bool) {
	type deleteCandidate struct {
		name    string
		machine *Machine
	}

	p.machinesMu.Lock()
	candidates := make([]deleteCandidate, 0, len(p.machines))
	for name, machine := range p.machines {
		if machine.stopping {
			continue
		}

		candidates = append(candidates, deleteCandidate{name: name, machine: machine})
	}
	p.machinesMu.Unlock()

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].machine.CreatedAt.Equal(candidates[j].machine.CreatedAt) {
			return candidates[i].name < candidates[j].name
		}
		return candidates[i].machine.CreatedAt.Before(candidates[j].machine.CreatedAt)
	})

	for _, candidate := range candidates {
		stateCtx, cancel := context.WithTimeout(ctx, scaleDownStateTimeout)
		idle, state, err := candidate.machine.IsIdleForScaleDown(stateCtx)
		cancel()
		if err != nil {
			p.logger.Debug().
				Str("machine", candidate.name).
				Err(err).
				Msg("Skipping scale down candidate because runner state is unavailable")
			continue
		}
		if !idle {
			p.logger.Debug().
				Str("machine", candidate.name).
				Str("runner_state", state).
				Msg("Skipping scale down candidate because runner is not idle")
			continue
		}

		p.machinesMu.Lock()
		current, exists := p.machines[candidate.name]
		if !exists || current != candidate.machine || current.stopping {
			p.machinesMu.Unlock()
			continue
		}

		current.stopping = true
		p.machinesMu.Unlock()
		return current, candidate.name, true
	}

	return nil, "", false
}

// removeMachine removes a single machine from the pool.
func (p *Pool) deleteMachine(ctx context.Context) error {
	targetMachine, targetName, ok := p.beginDeleteMachine(ctx)
	if !ok {
		return fmt.Errorf("no machines available to scale down")
	}

	return p.stopSelectedMachine(targetMachine, targetName)
}

func (p *Pool) stopSelectedMachine(targetMachine *Machine, targetName string) error {
	runnerRemoved := false
	ctx, cancel := context.WithTimeout(context.Background(), scaleDownRunnerAPITimeout)
	err := p.removeGitHubRunner(ctx, targetName, targetMachine.RunnerID)
	cancel()
	if err != nil {
		p.machinesMu.Lock()
		current, exists := p.machines[targetName]
		if exists && current == targetMachine {
			current.stopping = false
		}
		p.machinesMu.Unlock()
		if errors.Is(err, errBusyRunner) {
			return err
		}
		p.logger.Warn().Err(err).Msgf("Failed to deregister runner %s before stopping VM", targetName)
		return err
	}
	runnerRemoved = targetMachine.RunnerID != 0

	err = targetMachine.Stop()
	if err != nil {
		if !runnerRemoved {
			p.machinesMu.Lock()
			current, exists := p.machines[targetName]
			if exists && current == targetMachine {
				current.stopping = false
			}
			p.machinesMu.Unlock()
		}
		p.logger.Warn().Err(err).Msgf("Failed to stop VM %s", targetName)
		return err
	}

	p.logger.Info().Msgf("Successfully removed VM %s", targetName)
	return nil
}

// createSnapshot creates a snapshot of the specified image.
func (p *Pool) createSnapshot(ctx context.Context, image containerd.Image, snapshotID string) ([]mount.Mount, error) {
	snapshotService := p.containerd.SnapshotService(defaultSnapshotter)
	snapshotExists := true
	_, err := snapshotService.Stat(ctx, snapshotID)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, err
		}

		snapshotExists = false
	}

	if !snapshotExists {
		imageContent, err := image.RootFS(ctx)
		if err != nil {
			return nil, fmt.Errorf("image: rootfs: %w", err)
		}

		_, err = snapshotService.Prepare(ctx, snapshotID, identity.ChainID(imageContent).String())
		if err != nil {
			return nil, fmt.Errorf("prepare: %w", err)
		}
	}

	mounts, err := snapshotService.Mounts(ctx, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("mounts: %w", err)
	}

	return mounts, nil
}

func (p *Pool) prepareRootFSSize(ctx context.Context, rootDevice, runnerName string) error {
	initialSize := strings.TrimSpace(p.config.Firecracker.RootFSInitialSize)
	maxSize := strings.TrimSpace(p.config.Firecracker.RootFSMaxSize)
	if initialSize == "" && maxSize == "" {
		return nil
	}

	currentSizeBytes, err := blockDeviceSize(ctx, rootDevice)
	if err != nil {
		return err
	}

	if initialSize != "" {
		initialSizeBytes, err := parseRootFSSize(initialSize)
		if err != nil {
			return fmt.Errorf("rootfs_initial_size: %w", err)
		}
		switch {
		case currentSizeBytes > initialSizeBytes:
			return fmt.Errorf("root device %s is %s, larger than rootfs_initial_size %s; lower containerd base_image_size instead of shrinking at boot", rootDevice, units.HumanSize(float64(currentSizeBytes)), initialSize)
		case currentSizeBytes < initialSizeBytes:
			if err := resizeRootBlockDevice(ctx, rootDevice, initialSizeBytes); err != nil {
				return fmt.Errorf("grow root block device to initial size: %w", err)
			}
			if err := p.resizeExt4RootFS(ctx, rootDevice, runnerName, initialSize); err != nil {
				return fmt.Errorf("grow root filesystem to initial size: %w", err)
			}
			p.logger.Info().
				Str("runner", runnerName).
				Str("root_device", rootDevice).
				Str("rootfs_initial_size", initialSize).
				Msg("Grew Firecracker rootfs to configured initial size before boot")

			currentSizeBytes, err = blockDeviceSize(ctx, rootDevice)
			if err != nil {
				return err
			}
		}
	}

	if maxSize == "" {
		return nil
	}

	maxSizeBytes, err := parseRootFSSize(maxSize)
	if err != nil {
		return fmt.Errorf("rootfs_max_size: %w", err)
	}
	if currentSizeBytes > maxSizeBytes {
		return fmt.Errorf("root device %s is %s, larger than rootfs_max_size %s", rootDevice, units.HumanSize(float64(currentSizeBytes)), maxSize)
	}
	if currentSizeBytes == maxSizeBytes {
		return nil
	}

	if err := resizeRootBlockDevice(ctx, rootDevice, maxSizeBytes); err != nil {
		return fmt.Errorf("grow root block device to max size: %w", err)
	}

	p.logger.Info().
		Str("runner", runnerName).
		Str("root_device", rootDevice).
		Str("rootfs_initial_size", initialSize).
		Str("rootfs_max_size", maxSize).
		Msg("Expanded Firecracker root block device ceiling before boot")

	return nil
}

func (p *Pool) resizeExt4RootFS(ctx context.Context, rootDevice, runnerName, targetSize string) error {
	for _, args := range [][]string{
		{"e2fsck", "-fy", rootDevice},
		{"resize2fs", rootDevice, targetSize},
		{"e2fsck", "-fy", rootDevice},
	} {
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			exitErr, ok := err.(*exec.ExitError)
			if args[0] == "e2fsck" && ok && exitErr.ExitCode() == 1 {
				p.logger.Info().
					Str("runner", runnerName).
					Str("command", strings.Join(args, " ")).
					Msg("e2fsck corrected Firecracker rootfs before grow")
				continue
			}
			return fmt.Errorf("%s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
	}

	return nil
}

func parseRootFSSize(raw string) (uint64, error) {
	sizeBytes, err := units.RAMInBytes(raw)
	if err != nil {
		return 0, err
	}
	if sizeBytes <= 0 {
		return 0, fmt.Errorf("size must be positive")
	}
	if sizeBytes%512 != 0 {
		return 0, fmt.Errorf("size must align to 512-byte sectors")
	}
	return uint64(sizeBytes), nil
}

func blockDeviceSize(ctx context.Context, device string) (uint64, error) {
	output, err := exec.CommandContext(ctx, "blockdev", "--getsize64", device).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("blockdev --getsize64 %s: %w: %s", device, err, strings.TrimSpace(string(output)))
	}
	size, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse blockdev size for %s: %w", device, err)
	}
	return size, nil
}

func resizeRootBlockDevice(ctx context.Context, rootDevice string, targetSizeBytes uint64) error {
	deviceName, err := devmapperDeviceName(rootDevice)
	if err != nil {
		return err
	}

	output, err := exec.CommandContext(ctx, "dmsetup", "table", deviceName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("dmsetup table %s: %w: %s", deviceName, err, strings.TrimSpace(string(output)))
	}

	targetTable, err := growDevmapperThinTable(strings.TrimSpace(string(output)), targetSizeBytes)
	if err != nil {
		return err
	}

	if targetTable == "" {
		return nil
	}

	if output, err := exec.CommandContext(ctx, "dmsetup", "suspend", deviceName).CombinedOutput(); err != nil {
		return fmt.Errorf("dmsetup suspend %s: %w: %s", deviceName, err, strings.TrimSpace(string(output)))
	}

	reloadOutput, reloadErr := exec.CommandContext(ctx, "dmsetup", "reload", deviceName, "--table", targetTable).CombinedOutput()
	resumeOutput, resumeErr := exec.CommandContext(ctx, "dmsetup", "resume", deviceName).CombinedOutput()
	if reloadErr != nil {
		return fmt.Errorf("dmsetup reload %s: %w: %s", deviceName, reloadErr, strings.TrimSpace(string(reloadOutput)))
	}
	if resumeErr != nil {
		return fmt.Errorf("dmsetup resume %s: %w: %s", deviceName, resumeErr, strings.TrimSpace(string(resumeOutput)))
	}

	return nil
}

func devmapperDeviceName(device string) (string, error) {
	const mapperPrefix = "/dev/mapper/"
	if !strings.HasPrefix(device, mapperPrefix) {
		return "", fmt.Errorf("expected devmapper root device under %s, got %s", mapperPrefix, device)
	}

	name := strings.TrimPrefix(device, mapperPrefix)
	if name == "" || strings.ContainsRune(name, '/') {
		return "", fmt.Errorf("invalid devmapper root device: %s", device)
	}

	return name, nil
}

func growDevmapperThinTable(table string, targetSizeBytes uint64) (string, error) {
	fields := strings.Fields(table)
	if len(fields) < 5 {
		return "", fmt.Errorf("unexpected devmapper table: %q", table)
	}
	if fields[0] != "0" || fields[2] != "thin" {
		return "", fmt.Errorf("expected a single thin target table, got: %q", table)
	}

	currentSectors, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return "", fmt.Errorf("parse devmapper table size: %w", err)
	}
	targetSectors := targetSizeBytes / 512

	if currentSectors > targetSectors {
		return "", fmt.Errorf("devmapper device is already larger than target: current=%d sectors target=%d sectors", currentSectors, targetSectors)
	}
	if currentSectors == targetSectors {
		return "", nil
	}

	fields[1] = strconv.FormatUint(targetSectors, 10)
	return strings.Join(fields, " "), nil
}

// removeGitHubRunner removes a runner from GitHub Actions.
func (p *Pool) removeGitHubRunner(ctx context.Context, runnerName string, runnerID int64) error {
	if runnerID == 0 {
		return nil
	}

	if p.removeRunnerFn != nil {
		err := p.removeRunnerFn(ctx, runnerName, runnerID)
		if err == nil {
			return nil
		}
		switch {
		case isGitHubResponseStatus(err, 404):
			return nil
		case isGitHubResponseStatus(err, 422):
			return errBusyRunner
		default:
			return err
		}
	}

	if p.installationID.Load() == 0 {
		return fmt.Errorf("no installation ID available, cannot delete runner %s", runnerName)
	}

	client := p.github.Installation(p.installationID.Load())
	_, err := client.Actions.RemoveOrganizationRunner(ctx, p.config.Runner.Organization, runnerID)
	if err != nil {
		switch {
		case isGitHubResponseStatus(err, 404):
			p.logger.Debug().Msgf("GitHub runner %s (ID: %d) was already deleted", runnerName, runnerID)
			return nil
		case isGitHubResponseStatus(err, 422):
			return errBusyRunner
		default:
			return err
		}
	}

	p.logger.Debug().Msgf("Successfully deleted GitHub runner %s (ID: %d)", runnerName, runnerID)
	return nil
}

func isGitHubResponseStatus(err error, statusCode int) bool {
	var responseErr *githubv63.ErrorResponse
	if !errors.As(err, &responseErr) || responseErr.Response == nil {
		return false
	}

	return responseErr.Response.StatusCode == statusCode
}

func init() {
	_ = log.SetLevel("panic")
}
