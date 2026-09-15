package concurrency

import (
	"context"
	"strings"
	"sync"
)

// Memory coordinates account admission within one process.
type Memory struct {
	mu              sync.Mutex
	accounts        map[string]*accountState
	maxTotalWaiters int
	totalWaiters    int
}

type accountState struct {
	active  int
	limit   int
	waiters []*waiter
}

type waiter struct {
	ctx    context.Context
	ready  chan acquireResult
	queued bool
}

type acquireResult struct {
	lease Lease
	err   error
}

type memoryLease struct {
	once      sync.Once
	owner     *Memory
	accountID string
}

// NewMemory creates a local coordinator with a process-wide waiter cap.
func NewMemory(maxTotalWaiters int) *Memory {
	if maxTotalWaiters < 0 {
		maxTotalWaiters = 0
	}
	return &Memory{
		accounts:        make(map[string]*accountState),
		maxTotalWaiters: maxTotalWaiters,
	}
}

// SetMaxTotalWaiters updates the process-wide waiter cap without evicting queued requests.
func (m *Memory) SetMaxTotalWaiters(maxTotalWaiters int) {
	if m == nil {
		return
	}
	if maxTotalWaiters < 0 {
		maxTotalWaiters = 0
	}
	m.mu.Lock()
	m.maxTotalWaiters = maxTotalWaiters
	m.mu.Unlock()
}

// BypassWaiters releases every queued request without assigning account capacity.
// It is used when the feature is disabled during a live configuration reload.
func (m *Memory) BypassWaiters() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for accountID, state := range m.accounts {
		if state == nil {
			continue
		}
		for _, wait := range state.waiters {
			if wait == nil || !wait.queued {
				continue
			}
			wait.queued = false
			if m.totalWaiters > 0 {
				m.totalWaiters--
			}
			wait.ready <- acquireResult{lease: &memoryLease{}}
		}
		state.waiters = nil
		m.cleanupLocked(accountID, state)
	}
}

// BypassAccount releases one account's queued requests without assigning capacity.
func (m *Memory) BypassAccount(accountID string) {
	m.finishAccountWaiters(accountID, nil)
}

// CancelAccount rejects queued requests because the account is no longer eligible.
func (m *Memory) CancelAccount(accountID string) {
	m.finishAccountWaiters(accountID, ErrAccountUnavailable)
}

// TryAcquire acquires immediately only when capacity exists and no waiter is ahead.
func (m *Memory) TryAcquire(accountID string, maxActive int) (Lease, bool) {
	if m == nil || strings.TrimSpace(accountID) == "" || maxActive <= 0 {
		return nil, false
	}
	accountID = strings.TrimSpace(accountID)
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.accountLocked(accountID)
	state.limit = maxActive
	m.grantLocked(accountID, state)
	if len(state.waiters) > 0 || state.active >= state.limit {
		return nil, false
	}
	state.active++
	return &memoryLease{owner: m, accountID: accountID}, true
}

// Acquire acquires immediately or joins the account FIFO queue until ctx ends.
func (m *Memory) Acquire(ctx context.Context, accountID string, limits Limits) (Lease, error) {
	if m == nil || strings.TrimSpace(accountID) == "" || limits.MaxActive <= 0 || limits.MaxWaiting < 0 {
		return nil, ErrInvalidLimits
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errCtx := ctx.Err(); errCtx != nil {
		return nil, errCtx
	}
	accountID = strings.TrimSpace(accountID)

	m.mu.Lock()
	state := m.accountLocked(accountID)
	state.limit = limits.MaxActive
	m.grantLocked(accountID, state)
	if len(state.waiters) == 0 && state.active < state.limit {
		state.active++
		lease := &memoryLease{owner: m, accountID: accountID}
		m.mu.Unlock()
		return lease, nil
	}
	if limits.MaxWaiting == 0 || len(state.waiters) >= limits.MaxWaiting {
		m.mu.Unlock()
		return nil, ErrAccountQueueFull
	}
	if m.maxTotalWaiters == 0 || m.totalWaiters >= m.maxTotalWaiters {
		m.mu.Unlock()
		return nil, ErrGlobalQueueFull
	}
	wait := &waiter{ctx: ctx, ready: make(chan acquireResult, 1), queued: true}
	state.waiters = append(state.waiters, wait)
	m.totalWaiters++
	m.mu.Unlock()

	select {
	case result := <-wait.ready:
		return result.lease, result.err
	case <-ctx.Done():
		m.mu.Lock()
		if wait.queued {
			m.removeWaiterLocked(state, wait)
			m.grantLocked(accountID, state)
			m.cleanupLocked(accountID, state)
			m.mu.Unlock()
			return nil, ctx.Err()
		}
		m.mu.Unlock()
		result := <-wait.ready
		if result.lease != nil {
			result.lease.Release()
		}
		return nil, ctx.Err()
	}
}

// UpdateLimit changes one account's capacity and grants queued requests when it grows.
func (m *Memory) UpdateLimit(accountID string, maxActive int) {
	if m == nil || strings.TrimSpace(accountID) == "" || maxActive <= 0 {
		return
	}
	accountID = strings.TrimSpace(accountID)
	m.mu.Lock()
	state := m.accountLocked(accountID)
	state.limit = maxActive
	m.grantLocked(accountID, state)
	m.mu.Unlock()
}

// Snapshot returns current counters without exposing mutable coordinator state.
func (m *Memory) Snapshot(accountID string) Snapshot {
	if m == nil {
		return Snapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := Snapshot{GlobalWaiting: m.totalWaiters}
	if state := m.accounts[strings.TrimSpace(accountID)]; state != nil {
		snapshot.Active = state.active
		snapshot.Waiting = len(state.waiters)
		snapshot.Limit = state.limit
	}
	return snapshot
}

func (l *memoryLease) Release() {
	if l == nil || l.owner == nil {
		return
	}
	l.once.Do(func() {
		l.owner.release(l.accountID)
	})
}

func (m *Memory) release(accountID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.accounts[accountID]
	if state == nil {
		return
	}
	if state.active > 0 {
		state.active--
	}
	m.grantLocked(accountID, state)
	m.cleanupLocked(accountID, state)
}

func (m *Memory) accountLocked(accountID string) *accountState {
	state := m.accounts[accountID]
	if state == nil {
		state = &accountState{}
		m.accounts[accountID] = state
	}
	return state
}

func (m *Memory) grantLocked(accountID string, state *accountState) {
	if state == nil || state.limit <= 0 {
		return
	}
	for state.active < state.limit && len(state.waiters) > 0 {
		wait := state.waiters[0]
		state.waiters = state.waiters[1:]
		if wait == nil || !wait.queued {
			continue
		}
		wait.queued = false
		if m.totalWaiters > 0 {
			m.totalWaiters--
		}
		if wait.ctx.Err() != nil {
			continue
		}
		state.active++
		wait.ready <- acquireResult{lease: &memoryLease{owner: m, accountID: accountID}}
	}
}

func (m *Memory) finishAccountWaiters(accountID string, err error) {
	if m == nil || strings.TrimSpace(accountID) == "" {
		return
	}
	accountID = strings.TrimSpace(accountID)
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.accounts[accountID]
	if state == nil {
		return
	}
	for _, wait := range state.waiters {
		if wait == nil || !wait.queued {
			continue
		}
		wait.queued = false
		if m.totalWaiters > 0 {
			m.totalWaiters--
		}
		result := acquireResult{err: err}
		if err == nil {
			result.lease = &memoryLease{}
		}
		wait.ready <- result
	}
	state.waiters = nil
	m.cleanupLocked(accountID, state)
}

func (m *Memory) removeWaiterLocked(state *accountState, target *waiter) {
	if state == nil || target == nil {
		return
	}
	for index, candidate := range state.waiters {
		if candidate != target {
			continue
		}
		state.waiters = append(state.waiters[:index], state.waiters[index+1:]...)
		target.queued = false
		if m.totalWaiters > 0 {
			m.totalWaiters--
		}
		return
	}
}

func (m *Memory) cleanupLocked(accountID string, state *accountState) {
	if state != nil && state.active == 0 && len(state.waiters) == 0 {
		delete(m.accounts, accountID)
	}
}
