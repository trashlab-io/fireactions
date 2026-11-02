package server

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapacityManagerReserveAndRelease(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      4,
	})

	reservations, reason := manager.ReserveUpTo(3, 1024, 1)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, reservations, 3)

	snapshot := manager.Snapshot()
	assert.Equal(t, int64(3072), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(3), snapshot.VCPUReserved)
	assert.Equal(t, 3, snapshot.Outstanding)

	reservations[0].Release()
	reservations[0].Release()

	snapshot = manager.Snapshot()
	assert.Equal(t, int64(2048), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(2), snapshot.VCPUReserved)
	assert.Equal(t, 2, snapshot.Outstanding)

	reservations[1].Release()
	reservations[2].Release()

	snapshot = manager.Snapshot()
	assert.Equal(t, int64(0), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(0), snapshot.VCPUReserved)
	assert.Equal(t, 0, snapshot.Outstanding)
}

func TestCapacityManagerPartialAdmissionAndReason(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 4096,
		VCPULimit:      8,
	})

	reservations, reason := manager.ReserveUpTo(3, 2048, 1)
	require.Equal(t, CapacityBlockReasonMemory, reason)
	require.Len(t, reservations, 2)

	for _, reservation := range reservations {
		reservation.Release()
	}

	manager = NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 8192,
		VCPULimit:      2,
	})

	reservations, reason = manager.ReserveUpTo(3, 1024, 1)
	require.Equal(t, CapacityBlockReasonVCPU, reason)
	require.Len(t, reservations, 2)

	for _, reservation := range reservations {
		reservation.Release()
	}

	manager = NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 2048,
		VCPULimit:      2,
	})

	reservations, reason = manager.ReserveUpTo(2, 2048, 2)
	require.Equal(t, CapacityBlockReasonBoth, reason)
	require.Len(t, reservations, 1)
}

func TestCapacityManagerDisabledDimensions(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{})

	reservations, reason := manager.ReserveUpTo(5, 2048, 4)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, reservations, 5)

	snapshot := manager.Snapshot()
	assert.Equal(t, int64(10240), snapshot.MemoryReservedMib)
	assert.Equal(t, int64(20), snapshot.VCPUReserved)
	assert.Equal(t, 5, snapshot.Outstanding)
}

func TestCapacityManagerConcurrentReservationsNeverExceedLimit(t *testing.T) {
	manager := NewCapacityManager(&CapacityConfig{
		MemoryLimitMib: 8192,
		VCPULimit:      8,
	})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservations, _ := manager.ReserveUpTo(1, 1024, 1)
			for _, reservation := range reservations {
				reservation.Release()
			}
		}()
	}

	wg.Wait()

	snapshot := manager.Snapshot()
	assert.LessOrEqual(t, snapshot.MemoryReservedMib, int64(8192))
	assert.LessOrEqual(t, snapshot.VCPUReserved, int64(8))
	assert.Equal(t, 0, snapshot.Outstanding)
}

// fakeClock lets tests move time for the waiting order.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newFairCapacityManager(memoryLimitMib, vcpuLimit int64) (*CapacityManager, *fakeClock) {
	manager := NewCapacityManager(&CapacityConfig{MemoryLimitMib: memoryLimitMib, VCPULimit: vcpuLimit})
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	manager.now = clock.Now
	return manager, clock
}

func TestCapacityManagerHoldsFreedCapacityForTheLongestWaitingPool(t *testing.T) {
	manager, clock := newFairCapacityManager(4096, 0)

	small, reason := manager.ReserveFor("small", 4, 1024, 1)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, small, 4)

	// The large pool cannot fit and starts waiting.
	large, reason := manager.ReserveFor("large", 1, 2048, 1)
	require.Empty(t, large)
	require.Equal(t, CapacityBlockReasonMemory, reason)

	// One small VM finishes. Without the waiting order the small pool would
	// take the freed 1 GiB again; now it is held for the large pool.
	clock.Advance(time.Second)
	small[0].Release()
	got, reason := manager.ReserveFor("small", 1, 1024, 1)
	require.Empty(t, got)
	require.Equal(t, CapacityBlockReasonWaiting, reason)

	// A second small VM finishes: 2 GiB are free, still held for the large pool.
	clock.Advance(time.Second)
	small[1].Release()
	got, _ = manager.ReserveFor("small", 1, 1024, 1)
	require.Empty(t, got)

	// The large pool asks again and gets the VM it has been waiting for.
	large, reason = manager.ReserveFor("large", 1, 2048, 1)
	require.Equal(t, CapacityBlockReasonNone, reason)
	require.Len(t, large, 1)
}

func TestCapacityManagerPoolGoesToTheBackOfTheLineAfterAGrant(t *testing.T) {
	manager, clock := newFairCapacityManager(4096, 0)

	held, _ := manager.ReserveFor("a", 4, 1024, 1)
	require.Len(t, held, 4)

	// b waits first, then c.
	_, _ = manager.ReserveFor("b", 1, 2048, 1)
	clock.Advance(time.Second)
	_, _ = manager.ReserveFor("c", 1, 2048, 1)

	// 2 GiB free: b is first in line and gets it, c is held back.
	held[0].Release()
	held[1].Release()
	clock.Advance(time.Second)
	got, reason := manager.ReserveFor("c", 1, 2048, 1)
	require.Empty(t, got)
	require.Equal(t, CapacityBlockReasonWaiting, reason)
	got, _ = manager.ReserveFor("b", 2, 2048, 1)
	require.Len(t, got, 1, "b still wants one more, so it keeps waiting, now behind c")

	// The next 2 GiB go to c, which has now waited longest, not to b.
	held[2].Release()
	held[3].Release()
	clock.Advance(time.Second)
	got, reason = manager.ReserveFor("b", 1, 2048, 1)
	require.Empty(t, got)
	require.Equal(t, CapacityBlockReasonWaiting, reason)
	got, _ = manager.ReserveFor("c", 1, 2048, 1)
	require.Len(t, got, 1)
}

func TestCapacityManagerOnlyHoldsOneVMOfTheWaitingPool(t *testing.T) {
	manager, _ := newFairCapacityManager(8192, 0)

	held, _ := manager.ReserveFor("small", 8, 1024, 1)
	require.Len(t, held, 8)
	_, _ = manager.ReserveFor("large", 1, 2048, 1)

	// 4 GiB free: 2 GiB are held for the large pool, the rest is usable.
	for _, reservation := range held[:4] {
		reservation.Release()
	}
	got, reason := manager.ReserveFor("small", 4, 1024, 1)
	require.Len(t, got, 2)
	require.Equal(t, CapacityBlockReasonWaiting, reason)
}

func TestCapacityManagerForgetsWaitingPools(t *testing.T) {
	manager, clock := newFairCapacityManager(4096, 0)

	held, _ := manager.ReserveFor("small", 4, 1024, 1)
	_, _ = manager.ReserveFor("large", 1, 2048, 1)
	held[0].Release()

	// The large pool's demand goes away.
	manager.Forget("large")
	got, reason := manager.ReserveFor("small", 1, 1024, 1)
	require.Len(t, got, 1)
	require.Equal(t, CapacityBlockReasonNone, reason)

	// A pool that stops asking stops holding capacity after the TTL.
	_, _ = manager.ReserveFor("large", 1, 2048, 1)
	held[1].Release()
	got, _ = manager.ReserveFor("small", 1, 1024, 1)
	require.Empty(t, got)
	clock.Advance(capacityWaiterTTL + time.Second)
	got, _ = manager.ReserveFor("small", 1, 1024, 1)
	require.Len(t, got, 1)
}

func TestCapacityManagerIgnoresWaitersThatCanNeverFit(t *testing.T) {
	manager, _ := newFairCapacityManager(4096, 4)

	_, reason := manager.ReserveFor("huge", 1, 8192, 1)
	require.Equal(t, CapacityBlockReasonMemory, reason)

	got, reason := manager.ReserveFor("small", 4, 1024, 1)
	require.Len(t, got, 4)
	require.Equal(t, CapacityBlockReasonNone, reason)
}
