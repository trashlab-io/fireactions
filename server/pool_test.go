package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	githubv63 "github.com/google/go-github/v63/github"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolScaleUpHonorsSharedCapacity(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      4,
	})

	poolA := newTestPool(t, "pool-a", 2048, 2, capacity)
	poolB := newTestPool(t, "pool-b", 2048, 2, capacity)

	runtimes := make([]*fakeMachineRuntime, 0, 2)
	var runtimesMu sync.Mutex

	attachMachine := func(pool *Pool, prefix string) {
		var created atomic.Int32
		pool.createMachineFn = func(ctx context.Context, reservation *CapacityReservation) error {
			runtime := newFakeMachineRuntime()
			runtimesMu.Lock()
			runtimes = append(runtimes, runtime)
			runtimesMu.Unlock()

			name := prefix + "-" + time.Now().Format("150405.000000")
			machine := &Machine{
				Name:        name,
				Pool:        pool.config.Name,
				CreatedAt:   time.Now().UTC(),
				MemoryMib:   pool.config.Firecracker.MachineConfig.MemSizeMib,
				VCPUCount:   pool.config.Firecracker.MachineConfig.VcpuCount,
				Reservation: reservation,
				waitFunc:    runtime.wait,
				stopFunc:    runtime.stop,
			}

			pool.trackMachine(machine)
			created.Add(1)
			return nil
		}
	}

	attachMachine(poolA, "a")
	attachMachine(poolB, "b")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, poolA.Scale(context.Background(), 2))
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, poolB.Scale(context.Background(), 2))
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		return poolA.pendingCreates.Load() == 0 && poolB.pendingCreates.Load() == 0
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return poolA.GetCurrentSize()+poolB.GetCurrentSize() == 2
	}, time.Second, 10*time.Millisecond)

	snapshot := capacity.Snapshot()
	assert.Equal(t, int64(4096), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(4), snapshot.VCPUReserved)
	assert.Equal(t, 2, snapshot.Outstanding)

	runtimesMu.Lock()
	currentRuntimes := append([]*fakeMachineRuntime(nil), runtimes...)
	runtimesMu.Unlock()
	for _, runtime := range currentRuntimes {
		runtime.stop()
	}

	require.Eventually(t, func() bool {
		snapshot := capacity.Snapshot()
		return snapshot.MemoryReservedMib == 0 && snapshot.VCPUReserved == 0 && snapshot.Outstanding == 0
	}, time.Second, 10*time.Millisecond)
}

func TestPoolScaleUpLetsALargePoolInAheadOfASmallOne(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      8,
	})
	small := newTestPool(t, "pool-small", 1024, 1, capacity)
	large := newTestPool(t, "pool-large", 2048, 1, capacity)

	var runtimesMu sync.Mutex
	var smallRuntimes []*fakeMachineRuntime
	attach := func(pool *Pool, prefix string) {
		var created atomic.Int32
		pool.createMachineFn = func(ctx context.Context, reservation *CapacityReservation) error {
			runtime := newFakeMachineRuntime()
			if pool == small {
				runtimesMu.Lock()
				smallRuntimes = append(smallRuntimes, runtime)
				runtimesMu.Unlock()
			}
			pool.trackMachine(&Machine{
				Name:        fmt.Sprintf("%s-%d", prefix, created.Add(1)),
				Pool:        pool.config.Name,
				CreatedAt:   time.Now().UTC(),
				MemoryMib:   pool.config.Firecracker.MachineConfig.MemSizeMib,
				VCPUCount:   pool.config.Firecracker.MachineConfig.VcpuCount,
				Reservation: reservation,
				waitFunc:    runtime.wait,
				stopFunc:    runtime.stop,
			})
			return nil
		}
	}
	attach(small, "small")
	attach(large, "large")

	// The small pool fills the host, then the large pool asks and waits.
	require.NoError(t, small.Scale(context.Background(), 4))
	require.Eventually(t, func() bool { return small.GetCurrentSize() == 4 }, time.Second, 10*time.Millisecond)
	require.NoError(t, large.Scale(context.Background(), 1))
	assert.Equal(t, CapacityBlockReasonMemory, large.capacityBlockedReason)

	stopSmall := func(i int) {
		runtimesMu.Lock()
		runtime := smallRuntimes[i]
		runtimesMu.Unlock()
		runtime.stop()
		require.Eventually(t, func() bool { return small.GetCurrentSize() == 3-i }, time.Second, 10*time.Millisecond)
	}

	// Small VMs finish one by one. The small pool still wants 4, but the freed
	// memory is held until the large VM fits.
	stopSmall(0)
	require.NoError(t, small.Scale(context.Background(), 4))
	assert.Equal(t, CapacityBlockReasonWaiting, small.capacityBlockedReason)
	stopSmall(1)
	require.NoError(t, small.Scale(context.Background(), 4))
	assert.Equal(t, 2, small.GetCurrentSize()+int(small.pendingCreates.Load()))

	require.NoError(t, large.Scale(context.Background(), 1))
	require.Eventually(t, func() bool { return large.GetCurrentSize() == 1 }, time.Second, 10*time.Millisecond)
	assert.Equal(t, CapacityBlockReasonNone, large.capacityBlockedReason)
}

func TestPoolScaleUpBlocksWithoutIncrementingPendingCreatesForDeniedVMs(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})
	pool := newTestPool(t, "pool-blocked", 2048, 2, capacity)

	runtime := newFakeMachineRuntime()
	pool.createMachineFn = func(ctx context.Context, reservation *CapacityReservation) error {
		machine := &Machine{
			Name:        "blocked-1",
			Pool:        pool.config.Name,
			CreatedAt:   time.Now().UTC(),
			MemoryMib:   pool.config.Firecracker.MachineConfig.MemSizeMib,
			VCPUCount:   pool.config.Firecracker.MachineConfig.VcpuCount,
			Reservation: reservation,
			waitFunc:    runtime.wait,
			stopFunc:    runtime.stop,
		}
		pool.trackMachine(machine)
		return nil
	}

	beforeBlocked := testutil.ToFloat64(metricScaleOperations.WithLabelValues(pool.config.Name, pool.config.Runner.Organization, "up", "blocked"))

	require.NoError(t, pool.Scale(context.Background(), 2))

	require.Eventually(t, func() bool {
		return pool.pendingCreates.Load() == 0 && pool.GetCurrentSize() == 1
	}, time.Second, 10*time.Millisecond)

	snapshot := capacity.Snapshot()
	assert.Equal(t, int64(2048), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(2), snapshot.VCPUReserved)
	assert.Equal(t, 1, snapshot.Outstanding)
	assert.Equal(t, CapacityBlockReasonBoth, pool.capacityBlockedReason)

	afterBlocked := testutil.ToFloat64(metricScaleOperations.WithLabelValues(pool.config.Name, pool.config.Runner.Organization, "up", "blocked"))
	assert.Equal(t, 1.0, afterBlocked-beforeBlocked)

	runtime.stop()
	require.Eventually(t, func() bool {
		return capacity.Snapshot().Outstanding == 0
	}, time.Second, 10*time.Millisecond)
}

func TestPoolDesiredReplicasUsesMaxOfBaseAndDemand(t *testing.T) {
	pool := newTestPool(t, "pool-demand", 2048, 2, NewCapacityManager(nil))

	pool.SetReplicas(1)
	assert.Equal(t, 1, pool.GetBaseReplicas())
	assert.Equal(t, 1, pool.GetDesiredReplicas())

	pool.SetDemandReplicas(3)
	assert.Equal(t, 3, pool.GetDesiredReplicas())

	pool.SetDemandReplicas(0)
	assert.Equal(t, 1, pool.GetDesiredReplicas())
}

func TestPoolCreateFailureReleasesCapacity(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})
	pool := newTestPool(t, "pool-failure", 2048, 2, capacity)

	pool.createMachineFn = func(ctx context.Context, reservation *CapacityReservation) error {
		reservation.Release()
		return errors.New("boom")
	}

	require.NoError(t, pool.Scale(context.Background(), 1))

	require.Eventually(t, func() bool {
		snapshot := capacity.Snapshot()
		return snapshot.MemoryReservedMib == 0 && snapshot.VCPUReserved == 0 && snapshot.Outstanding == 0
	}, time.Second, 10*time.Millisecond)
}

func TestPoolCreateFailureBeforeCleanupReleasesCapacity(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})
	pool := newTestPool(t, "pool-early-failure", 2048, 2, capacity)

	// Like createMachine failing in ensureImage: it returns before it takes
	// ownership of the reservation.
	pool.createMachineFn = func(ctx context.Context, reservation *CapacityReservation) error {
		return errors.New("ensuring image: not found")
	}

	require.NoError(t, pool.Scale(context.Background(), 1))

	require.Eventually(t, func() bool {
		snapshot := capacity.Snapshot()
		return snapshot.MemoryReservedMib == 0 && snapshot.VCPUReserved == 0 && snapshot.Outstanding == 0
	}, time.Second, 10*time.Millisecond)
}

func TestPoolDeleteMachineKeepsReservationOnStopFailure(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})
	pool := newTestPool(t, "pool-stop-failure", 2048, 2, capacity)

	reservations, reason := capacity.ReserveUpTo(1, 2048, 2)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, reservations, 1)

	machine := &Machine{
		Name:        "failing-machine",
		Pool:        pool.config.Name,
		CreatedAt:   time.Now().UTC(),
		MemoryMib:   2048,
		VCPUCount:   2,
		Reservation: reservations[0],
		waitFunc: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
		runnerStateFn: func(context.Context) (string, error) {
			return "Idle", nil
		},
		stopFunc: func() error {
			return errors.New("stop failed")
		},
	}

	pool.machinesMu.Lock()
	pool.machines[machine.Name] = machine
	pool.machinesMu.Unlock()

	err := pool.deleteMachine(context.Background())
	require.Error(t, err)

	pool.machinesMu.Lock()
	current := pool.machines[machine.Name]
	pool.machinesMu.Unlock()

	require.NotNil(t, current)
	assert.False(t, current.stopping)

	snapshot := capacity.Snapshot()
	assert.Equal(t, int64(2048), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(2), snapshot.VCPUReserved)
	assert.Equal(t, 1, snapshot.Outstanding)

	reservations[0].Release()
}

func TestPoolScaleIgnoresDrainingMachinesInEffectiveSize(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      4,
	})
	pool := newTestPool(t, "pool-draining", 2048, 2, capacity)

	draining := &Machine{
		Name:      "draining",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC(),
		stopping:  true,
	}

	var activeStopCalls atomic.Int32
	active := &Machine{
		Name:      "active",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC(),
		stopFunc: func() error {
			activeStopCalls.Add(1)
			return nil
		},
	}

	pool.machinesMu.Lock()
	pool.machines[draining.Name] = draining
	pool.machines[active.Name] = active
	pool.machinesMu.Unlock()

	require.NoError(t, pool.Scale(context.Background(), 1))
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int32(0), activeStopCalls.Load())
}

func TestPoolTrackMachineContinuesCleanupAfterShutdownTimeout(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})
	pool := newTestPool(t, "pool-timeout-cleanup", 2048, 2, capacity)
	pool.shutdownWaitTimeout = 10 * time.Millisecond

	reservations, reason := capacity.ReserveUpTo(1, 2048, 2)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, reservations, 1)

	var leaseCanceled atomic.Int32
	waitReturned := make(chan struct{})
	machine := &Machine{
		Name:        "slow-exit",
		Pool:        pool.config.Name,
		CreatedAt:   time.Now().UTC(),
		Reservation: reservations[0],
		waitFunc: func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			close(waitReturned)
			return nil
		},
		leaseCancel: func(context.Context) error {
			leaseCanceled.Add(1)
			return nil
		},
	}

	pool.trackMachine(machine)
	pool.cancel()

	require.Eventually(t, func() bool {
		snapshot := capacity.Snapshot()
		return snapshot.Outstanding == 0 && leaseCanceled.Load() == 1
	}, time.Second, 10*time.Millisecond)

	pool.machinesMu.Lock()
	_, exists := pool.machines[machine.Name]
	pool.machinesMu.Unlock()
	assert.False(t, exists)

	<-waitReturned
}

func TestPoolScaleDoesNotDoubleCountSlowDelete(t *testing.T) {
	capacity := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      4,
	})
	pool := newTestPool(t, "pool-slow-delete", 2048, 2, capacity)

	stopRelease := make(chan struct{})
	var stopCalls atomic.Int32
	machine := &Machine{
		Name:      "slow-delete",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC(),
		runnerStateFn: func(context.Context) (string, error) {
			return "Idle", nil
		},
		stopFunc: func() error {
			stopCalls.Add(1)
			<-stopRelease
			return nil
		},
	}

	pool.machinesMu.Lock()
	pool.machines[machine.Name] = machine
	pool.machinesMu.Unlock()

	require.NoError(t, pool.Scale(context.Background(), 0))

	require.Eventually(t, func() bool {
		return stopCalls.Load() == 1
	}, time.Second, 10*time.Millisecond)

	// Reconcile again while Stop() is still in flight. The same delete must not
	// be counted twice and no replacement VM should be created.
	require.NoError(t, pool.Scale(context.Background(), 0))
	assert.Equal(t, int32(1), stopCalls.Load())

	close(stopRelease)
}

func TestPoolScaleDownSelectsIdleMachinesOnly(t *testing.T) {
	pool := newTestPool(t, "pool-busy-selection", 2048, 2, NewCapacityManager(nil))

	var idleStopCalls atomic.Int32
	idle := &Machine{
		Name:      "idle",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC().Add(-time.Minute),
		RunnerID:  11,
		runnerStateFn: func(context.Context) (string, error) {
			return "Idle", nil
		},
		stopFunc: func() error {
			idleStopCalls.Add(1)
			return nil
		},
	}

	var busyStopCalls atomic.Int32
	busy := &Machine{
		Name:      "busy",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC(),
		RunnerID:  22,
		runnerStateFn: func(context.Context) (string, error) {
			return "Running", nil
		},
		stopFunc: func() error {
			busyStopCalls.Add(1)
			return nil
		},
	}

	pool.removeRunnerFn = func(context.Context, string, int64) error { return nil }

	pool.machinesMu.Lock()
	pool.machines[idle.Name] = idle
	pool.machines[busy.Name] = busy
	pool.machinesMu.Unlock()

	require.NoError(t, pool.Scale(context.Background(), 0))

	require.Eventually(t, func() bool {
		return idleStopCalls.Load() == 1
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(0), busyStopCalls.Load())
	assert.False(t, busy.stopping)
}

func TestPoolScaleDownKeepsBusyMachineWhenDemandDrops(t *testing.T) {
	pool := newTestPool(t, "pool-busy-only", 2048, 2, NewCapacityManager(nil))

	var stopCalls atomic.Int32
	machine := &Machine{
		Name:      "busy-only",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC(),
		RunnerID:  33,
		runnerStateFn: func(context.Context) (string, error) {
			return "Running", nil
		},
		stopFunc: func() error {
			stopCalls.Add(1)
			return nil
		},
	}

	pool.removeRunnerFn = func(context.Context, string, int64) error { return nil }

	pool.machinesMu.Lock()
	pool.machines[machine.Name] = machine
	pool.machinesMu.Unlock()

	require.NoError(t, pool.Scale(context.Background(), 0))
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int32(0), stopCalls.Load())
	assert.False(t, machine.stopping)
}

func TestPoolDeleteMachineSkipsRunnerThatTurnsBusyDuringDelete(t *testing.T) {
	pool := newTestPool(t, "pool-race-delete", 2048, 2, NewCapacityManager(nil))

	var stopCalls atomic.Int32
	machine := &Machine{
		Name:      "race-delete",
		Pool:      pool.config.Name,
		CreatedAt: time.Now().UTC(),
		RunnerID:  44,
		runnerStateFn: func(context.Context) (string, error) {
			return "Idle", nil
		},
		stopFunc: func() error {
			stopCalls.Add(1)
			return nil
		},
	}

	pool.removeRunnerFn = func(context.Context, string, int64) error {
		return &githubv63.ErrorResponse{Response: &http.Response{StatusCode: 422}}
	}

	pool.machinesMu.Lock()
	pool.machines[machine.Name] = machine
	pool.machinesMu.Unlock()

	err := pool.deleteMachine(context.Background())
	require.ErrorIs(t, err, errBusyRunner)
	assert.Equal(t, int32(0), stopCalls.Load())
	assert.False(t, machine.stopping)
}

type fakeMachineRuntime struct {
	stopOnce sync.Once
	waitCh   chan struct{}
}

func newFakeMachineRuntime() *fakeMachineRuntime {
	return &fakeMachineRuntime{
		waitCh: make(chan struct{}),
	}
}

func (f *fakeMachineRuntime) wait(_ context.Context) error {
	<-f.waitCh
	return nil
}

func (f *fakeMachineRuntime) stop() error {
	f.stopOnce.Do(func() {
		close(f.waitCh)
	})
	return nil
}

func newTestPool(t *testing.T, name string, memoryMib, vcpuCount int64, capacity *CapacityManager) *Pool {
	t.Helper()

	logger := zerolog.New(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	var nextCID atomic.Uint32

	pool := &Pool{
		config: &PoolConfig{
			Name:           name,
			ShutdownOnExit: boolPtr(true),
			Replicas:       0,
			Runner: &RunnerConfig{
				Name:            name,
				ImagePullPolicy: "IfNotPresent",
				Image:           "test-image",
				Organization:    "test-org",
				GroupID:         1,
				Labels:          []string{"self-hosted", name},
			},
			Firecracker: &FirecrackerConfig{
				MachineConfig: FirecrackerMachineConfig{
					VcpuCount:  vcpuCount,
					MemSizeMib: memoryMib,
				},
			},
		},
		capacity:            capacity,
		machinesMu:          &sync.Mutex{},
		machines:            make(map[string]*Machine),
		logger:              &logger,
		l:                   &sync.Mutex{},
		isActive:            true,
		scaleTrigger:        make(chan struct{}, 1),
		stopCh:              make(chan struct{}, 1),
		doneCh:              make(chan struct{}),
		ctx:                 ctx,
		cancel:              cancel,
		nextCID:             &nextCID,
		shutdownWaitTimeout: 30 * time.Second,
	}
	pool.baseReplicas.Store(int32(pool.config.Replicas))
	pool.desiredReplicas.Store(int32(pool.config.Replicas))

	t.Cleanup(func() {
		cancel()
	})

	return pool
}

func boolPtr(v bool) *bool {
	return &v
}
