package concurrency

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

type testAcquireResult struct {
	name  string
	lease Lease
	err   error
}

func TestMemoryTryAcquireAndIdempotentRelease(t *testing.T) {
	coordinator := NewMemory(10)
	lease, acquired := coordinator.TryAcquire("account-a", 1)
	if !acquired || lease == nil {
		t.Fatal("first TryAcquire() did not acquire")
	}
	if _, acquired = coordinator.TryAcquire("account-a", 1); acquired {
		t.Fatal("second TryAcquire() acquired above limit")
	}
	lease.Release()
	lease.Release()
	if got := coordinator.Snapshot("account-a").Active; got != 0 {
		t.Fatalf("active = %d, want 0", got)
	}
}

func TestMemoryAcquireFIFOAndNoBypass(t *testing.T) {
	coordinator := NewMemory(10)
	lease, acquired := coordinator.TryAcquire("account-a", 1)
	if !acquired {
		t.Fatal("initial TryAcquire() did not acquire")
	}

	results := make(chan testAcquireResult, 2)
	startWaiter := func(name string) {
		go func() {
			waitLease, errAcquire := coordinator.Acquire(context.Background(), "account-a", Limits{MaxActive: 1, MaxWaiting: 2})
			results <- testAcquireResult{name: name, lease: waitLease, err: errAcquire}
		}()
	}
	startWaiter("first")
	waitForWaiters(t, coordinator, "account-a", 1)
	startWaiter("second")
	waitForWaiters(t, coordinator, "account-a", 2)

	if _, acquired = coordinator.TryAcquire("account-a", 1); acquired {
		t.Fatal("TryAcquire() bypassed queued waiters")
	}
	lease.Release()

	first := receiveResult(t, results)
	if first.err != nil || first.name != "first" {
		t.Fatalf("first granted result = %#v", first)
	}
	first.lease.Release()
	second := receiveResult(t, results)
	if second.err != nil || second.name != "second" {
		t.Fatalf("second granted result = %#v", second)
	}
	second.lease.Release()
}

func TestMemoryAcquireQueueLimits(t *testing.T) {
	coordinator := NewMemory(1)
	leaseA, _ := coordinator.TryAcquire("account-a", 1)
	leaseB, _ := coordinator.TryAcquire("account-b", 1)

	cancelA, waitA := startCancelableWait(t, coordinator, "account-a", Limits{MaxActive: 1, MaxWaiting: 1})
	defer cancelA()
	waitForWaiters(t, coordinator, "account-a", 1)

	_, errAccount := coordinator.Acquire(context.Background(), "account-a", Limits{MaxActive: 1, MaxWaiting: 1})
	if !errors.Is(errAccount, ErrAccountQueueFull) {
		t.Fatalf("account queue error = %v, want ErrAccountQueueFull", errAccount)
	}
	_, errGlobal := coordinator.Acquire(context.Background(), "account-b", Limits{MaxActive: 1, MaxWaiting: 1})
	if !errors.Is(errGlobal, ErrGlobalQueueFull) {
		t.Fatalf("global queue error = %v, want ErrGlobalQueueFull", errGlobal)
	}

	cancelA()
	if errWait := <-waitA; !errors.Is(errWait, context.Canceled) {
		t.Fatalf("canceled waiter error = %v", errWait)
	}
	leaseA.Release()
	leaseB.Release()
}

func TestMemoryAcquireCancellationRemovesWaiter(t *testing.T) {
	coordinator := NewMemory(10)
	lease, _ := coordinator.TryAcquire("account-a", 1)
	cancel, result := startCancelableWait(t, coordinator, "account-a", Limits{MaxActive: 1, MaxWaiting: 2})
	waitForWaiters(t, coordinator, "account-a", 1)
	cancel()
	if errWait := <-result; !errors.Is(errWait, context.Canceled) {
		t.Fatalf("Acquire() error = %v, want context.Canceled", errWait)
	}
	if got := coordinator.Snapshot("account-a").Waiting; got != 0 {
		t.Fatalf("waiting = %d, want 0", got)
	}
	lease.Release()
}

func TestMemoryUpdateLimitGrantsQueuedWaiter(t *testing.T) {
	coordinator := NewMemory(10)
	lease, _ := coordinator.TryAcquire("account-a", 1)
	result := make(chan Lease, 1)
	go func() {
		waitLease, _ := coordinator.Acquire(context.Background(), "account-a", Limits{MaxActive: 1, MaxWaiting: 1})
		result <- waitLease
	}()
	waitForWaiters(t, coordinator, "account-a", 1)

	coordinator.UpdateLimit("account-a", 2)
	granted := receiveLease(t, result)
	if got := coordinator.Snapshot("account-a").Active; got != 2 {
		t.Fatalf("active = %d, want 2", got)
	}
	granted.Release()
	lease.Release()
}

func TestMemoryBypassWaitersReleasesQueueWithoutConsumingSlots(t *testing.T) {
	coordinator := NewMemory(10)
	activeLease, _ := coordinator.TryAcquire("account-a", 1)
	result := make(chan Lease, 1)
	go func() {
		waitLease, _ := coordinator.Acquire(context.Background(), "account-a", Limits{MaxActive: 1, MaxWaiting: 1})
		result <- waitLease
	}()
	waitForWaiters(t, coordinator, "account-a", 1)
	coordinator.BypassWaiters()
	bypassLease := receiveLease(t, result)
	bypassLease.Release()
	snapshot := coordinator.Snapshot("account-a")
	if snapshot.Active != 1 || snapshot.Waiting != 0 || snapshot.GlobalWaiting != 0 {
		t.Fatalf("snapshot after bypass = %#v", snapshot)
	}
	activeLease.Release()
}

func TestMemoryCancelAccountRejectsQueuedWaiters(t *testing.T) {
	coordinator := NewMemory(10)
	activeLease, _ := coordinator.TryAcquire("account-a", 1)
	result := make(chan error, 1)
	go func() {
		_, errAcquire := coordinator.Acquire(context.Background(), "account-a", Limits{MaxActive: 1, MaxWaiting: 1})
		result <- errAcquire
	}()
	waitForWaiters(t, coordinator, "account-a", 1)
	coordinator.CancelAccount("account-a")
	if errAcquire := <-result; !errors.Is(errAcquire, ErrAccountUnavailable) {
		t.Fatalf("Acquire() error = %v, want ErrAccountUnavailable", errAcquire)
	}
	activeLease.Release()
}

func startCancelableWait(t *testing.T, coordinator *Memory, accountID string, limits Limits) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		lease, errAcquire := coordinator.Acquire(ctx, accountID, limits)
		if lease != nil {
			lease.Release()
		}
		result <- errAcquire
	}()
	return cancel, result
}

func waitForWaiters(t *testing.T, coordinator *Memory, accountID string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for coordinator.Snapshot(accountID).Waiting != count {
		if time.Now().After(deadline) {
			t.Fatalf("waiting count did not reach %d; snapshot=%#v", count, coordinator.Snapshot(accountID))
		}
		runtime.Gosched()
	}
}

func receiveLease(t *testing.T, result <-chan Lease) Lease {
	t.Helper()
	select {
	case lease := <-result:
		return lease
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for lease")
		return nil
	}
}

func receiveResult(t *testing.T, results <-chan testAcquireResult) testAcquireResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for acquisition result")
		return testAcquireResult{}
	}
}
