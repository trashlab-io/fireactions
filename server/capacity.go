package server

import (
	"sync"
	"time"
)

// capacityWaiterTTL drops a waiting pool that stopped asking for capacity, for
// example because its demand went away. Pools ask every few seconds while they
// are blocked.
const capacityWaiterTTL = 10 * time.Second

type CapacityBlockReason string

const (
	CapacityBlockReasonNone   CapacityBlockReason = "none"
	CapacityBlockReasonMemory CapacityBlockReason = "memory"
	CapacityBlockReasonVCPU   CapacityBlockReason = "vcpu"
	CapacityBlockReasonBoth   CapacityBlockReason = "both"
	// CapacityBlockReasonWaiting means capacity was free but is being held for
	// a pool that has waited longer.
	CapacityBlockReasonWaiting CapacityBlockReason = "waiting"
)

type CapacitySnapshot struct {
	MemoryLimitMib    int64
	MemoryReservedMib int64
	VCPULimit         int64
	VCPUReserved      int64
	Outstanding       int
}

type capacityReservationState struct {
	memoryMib int64
	vcpuCount int64
	released  bool
}

// capacityWaiter is a pool that asked for capacity and did not get all of it.
// since is when it last got capacity, or when it started waiting.
type capacityWaiter struct {
	memoryMib int64
	vcpuCount int64
	since     time.Time
	seen      time.Time
}

type CapacityReservation struct {
	manager   *CapacityManager
	id        uint64
	memoryMib int64
	vcpuCount int64
	release   sync.Once
}

func (r *CapacityReservation) Release() {
	if r == nil || r.manager == nil {
		return
	}

	r.release.Do(func() {
		r.manager.release(r.id)
	})
}

type CapacityManager struct {
	mu                sync.Mutex
	nextReservationID uint64
	memoryLimitMib    int64
	memoryReservedMib int64
	vcpuLimit         int64
	vcpuReserved      int64
	reservations      map[uint64]*capacityReservationState
	waiters           map[string]*capacityWaiter
	now               func() time.Time
}

func NewCapacityManager(cfg *CapacityConfig) *CapacityManager {
	manager := &CapacityManager{
		reservations: make(map[uint64]*capacityReservationState),
		waiters:      make(map[string]*capacityWaiter),
		now:          time.Now,
	}
	if cfg != nil {
		manager.memoryLimitMib = cfg.MemoryLimitMib
		manager.vcpuLimit = cfg.VCPULimit
	}

	metricCapacityMemoryLimit.Set(float64(manager.memoryLimitMib))
	metricCapacityMemoryReserved.Set(0)
	metricCapacityVCPULimit.Set(float64(manager.vcpuLimit))
	metricCapacityVCPUReserved.Set(0)

	return manager
}

func (m *CapacityManager) Enabled() bool {
	if m == nil {
		return false
	}

	return m.memoryLimitMib > 0 || m.vcpuLimit > 0
}

// ReserveUpTo reserves capacity for up to count VMs without taking part in the
// waiting order. It still leaves room for the pool that has waited longest.
func (m *CapacityManager) ReserveUpTo(count int, memoryMib, vcpuCount int64) ([]*CapacityReservation, CapacityBlockReason) {
	return m.ReserveFor("", count, memoryMib, vcpuCount)
}

// ReserveFor reserves capacity for up to count VMs of one pool.
//
// Without an order, freed capacity goes to whichever pool asks next, so pools
// with small VMs keep filling the gaps and pools with large VMs can wait
// indefinitely. A pool that does not get all it asked for is recorded as
// waiting. Other pools may then only use capacity beyond one VM of the pool
// that has gone longest without capacity, so freed capacity builds up until
// that VM fits. A pool that gets capacity goes to the back of the line.
func (m *CapacityManager) ReserveFor(pool string, count int, memoryMib, vcpuCount int64) ([]*CapacityReservation, CapacityBlockReason) {
	if m == nil || count <= 0 {
		return nil, CapacityBlockReasonNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	m.pruneWaitersLocked(now)

	freeMemory := m.memoryLimitMib - m.memoryReservedMib
	freeVCPU := m.vcpuLimit - m.vcpuReserved
	heldForWaiter := false
	if head := m.headWaiterLocked(pool); head != nil {
		freeMemory -= head.memoryMib
		freeVCPU -= head.vcpuCount
		heldForWaiter = true
	}

	allowed := count
	if m.memoryLimitMib > 0 {
		memorySlots := int(freeMemory / memoryMib)
		if memorySlots < 0 {
			memorySlots = 0
		}
		if memorySlots < allowed {
			allowed = memorySlots
		}
	}

	if m.vcpuLimit > 0 {
		vcpuSlots := int(freeVCPU / vcpuCount)
		if vcpuSlots < 0 {
			vcpuSlots = 0
		}
		if vcpuSlots < allowed {
			allowed = vcpuSlots
		}
	}

	if pool != "" {
		m.recordWaiterLocked(pool, now, allowed, count, memoryMib, vcpuCount)
	}

	reservations := make([]*CapacityReservation, 0, allowed)
	for i := 0; i < allowed; i++ {
		m.nextReservationID++
		id := m.nextReservationID
		m.reservations[id] = &capacityReservationState{
			memoryMib: memoryMib,
			vcpuCount: vcpuCount,
		}
		m.memoryReservedMib += memoryMib
		m.vcpuReserved += vcpuCount
		reservations = append(reservations, &CapacityReservation{
			manager:   m,
			id:        id,
			memoryMib: memoryMib,
			vcpuCount: vcpuCount,
		})
	}

	metricCapacityMemoryReserved.Set(float64(m.memoryReservedMib))
	metricCapacityVCPUReserved.Set(float64(m.vcpuReserved))

	if allowed == count {
		return reservations, CapacityBlockReasonNone
	}

	memoryBlocked := m.memoryLimitMib > 0 && m.memoryReservedMib+memoryMib > m.memoryLimitMib
	vcpuBlocked := m.vcpuLimit > 0 && m.vcpuReserved+vcpuCount > m.vcpuLimit

	switch {
	case heldForWaiter && !memoryBlocked && !vcpuBlocked:
		return reservations, CapacityBlockReasonWaiting
	case memoryBlocked && vcpuBlocked:
		return reservations, CapacityBlockReasonBoth
	case memoryBlocked:
		return reservations, CapacityBlockReasonMemory
	case vcpuBlocked:
		return reservations, CapacityBlockReasonVCPU
	default:
		return reservations, CapacityBlockReasonNone
	}
}

// Forget removes a pool from the waiting order, for example when its demand is
// met or gone.
func (m *CapacityManager) Forget(pool string) {
	if m == nil || pool == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.waiters, pool)
}

func (m *CapacityManager) recordWaiterLocked(pool string, now time.Time, allowed, count int, memoryMib, vcpuCount int64) {
	if allowed == count {
		delete(m.waiters, pool)
		return
	}

	waiter, ok := m.waiters[pool]
	if !ok {
		waiter = &capacityWaiter{since: now}
		m.waiters[pool] = waiter
	}
	if allowed > 0 {
		waiter.since = now
	}
	waiter.memoryMib = memoryMib
	waiter.vcpuCount = vcpuCount
	waiter.seen = now
}

func (m *CapacityManager) pruneWaitersLocked(now time.Time) {
	for pool, waiter := range m.waiters {
		if now.Sub(waiter.seen) > capacityWaiterTTL {
			delete(m.waiters, pool)
		}
	}
}

// headWaiterLocked returns the pool other than pool that has waited longest
// and has waited longer than pool itself. VMs that could never fit within the
// limits do not hold capacity back.
func (m *CapacityManager) headWaiterLocked(pool string) *capacityWaiter {
	var head *capacityWaiter
	for name, waiter := range m.waiters {
		if name == pool {
			continue
		}
		if m.memoryLimitMib > 0 && waiter.memoryMib > m.memoryLimitMib {
			continue
		}
		if m.vcpuLimit > 0 && waiter.vcpuCount > m.vcpuLimit {
			continue
		}
		if head == nil || waiter.since.Before(head.since) {
			head = waiter
		}
	}

	if head == nil {
		return nil
	}
	if own, ok := m.waiters[pool]; ok && !head.since.Before(own.since) {
		return nil
	}

	return head
}

func (m *CapacityManager) Snapshot() CapacitySnapshot {
	if m == nil {
		return CapacitySnapshot{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return CapacitySnapshot{
		MemoryLimitMib:    m.memoryLimitMib,
		MemoryReservedMib: m.memoryReservedMib,
		VCPULimit:         m.vcpuLimit,
		VCPUReserved:      m.vcpuReserved,
		Outstanding:       len(m.reservations),
	}
}

func (m *CapacityManager) release(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reservation, ok := m.reservations[id]
	if !ok || reservation.released {
		return
	}

	reservation.released = true
	m.memoryReservedMib -= reservation.memoryMib
	m.vcpuReserved -= reservation.vcpuCount
	delete(m.reservations, id)

	metricCapacityMemoryReserved.Set(float64(m.memoryReservedMib))
	metricCapacityVCPUReserved.Set(float64(m.vcpuReserved))
}
