package auth

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	accountconcurrency "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth/concurrency"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type accountConcurrencyTestExecutor struct {
	entered chan string
	release chan struct{}
}

func (e *accountConcurrencyTestExecutor) Identifier() string { return "codex" }

func (e *accountConcurrencyTestExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.entered <- auth.ID
	<-e.release
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (e *accountConcurrencyTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *accountConcurrencyTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *accountConcurrencyTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.entered <- "count"
	<-e.release
	return cliproxyexecutor.Response{Payload: []byte("1")}, nil
}

func (e *accountConcurrencyTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestManagerAccountConcurrencySerializesExecute(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(2 * time.Second))
	executor := &accountConcurrencyTestExecutor{entered: make(chan string, 2), release: make(chan struct{}, 2)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 2*time.Second)

	results := make(chan error, 2)
	go func() {
		_, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		results <- errExecute
	}()
	if got := receiveString(t, executor.entered); got != "account-a" {
		t.Fatalf("first execute auth = %q", got)
	}
	go func() {
		_, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		results <- errExecute
	}()
	waitForManagerWaiters(t, manager, "account-a", 1)

	executor.release <- struct{}{}
	if errFirst := receiveError(t, results); errFirst != nil {
		t.Fatalf("first Execute() error = %v", errFirst)
	}
	if got := receiveString(t, executor.entered); got != "account-a" {
		t.Fatalf("second execute auth = %q", got)
	}
	executor.release <- struct{}{}
	if errSecond := receiveError(t, results); errSecond != nil {
		t.Fatalf("second Execute() error = %v", errSecond)
	}
	if snapshot := manager.accountConcurrency.Snapshot("account-a"); snapshot.Active != 0 || snapshot.Waiting != 0 {
		t.Fatalf("final snapshot = %#v", snapshot)
	}
}

func TestManagerAccountConcurrencySerializesCount(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(2 * time.Second))
	executor := &accountConcurrencyTestExecutor{entered: make(chan string, 2), release: make(chan struct{}, 2)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 2*time.Second)

	results := make(chan error, 2)
	run := func() {
		_, errCount := manager.ExecuteCount(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		results <- errCount
	}
	go run()
	if got := receiveString(t, executor.entered); got != "count" {
		t.Fatalf("first CountTokens marker = %q", got)
	}
	go run()
	waitForManagerWaiters(t, manager, "account-a", 1)
	executor.release <- struct{}{}
	if errFirst := receiveError(t, results); errFirst != nil {
		t.Fatalf("first ExecuteCount() error = %v", errFirst)
	}
	if got := receiveString(t, executor.entered); got != "count" {
		t.Fatalf("second CountTokens marker = %q", got)
	}
	executor.release <- struct{}{}
	if errSecond := receiveError(t, results); errSecond != nil {
		t.Fatalf("second ExecuteCount() error = %v", errSecond)
	}
}

func TestManagerAccountConcurrencyDisabledPreservesParallelExecution(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &accountConcurrencyTestExecutor{entered: make(chan string, 2), release: make(chan struct{}, 2)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 2*time.Second)
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
			results <- errExecute
		}()
	}
	_ = receiveString(t, executor.entered)
	_ = receiveString(t, executor.entered)
	executor.release <- struct{}{}
	executor.release <- struct{}{}
	if errFirst := receiveError(t, results); errFirst != nil {
		t.Fatalf("first disabled Execute() error = %v", errFirst)
	}
	if errSecond := receiveError(t, results); errSecond != nil {
		t.Fatalf("second disabled Execute() error = %v", errSecond)
	}
	if snapshot := manager.accountConcurrency.Snapshot("account-a"); snapshot.Active != 0 || snapshot.Waiting != 0 {
		t.Fatalf("disabled coordinator snapshot = %#v", snapshot)
	}
}

func TestManagerAccountConcurrencyHomeBypass(t *testing.T) {
	cfg := accountConcurrencyTestConfig(time.Second)
	cfg.Home.Enabled = true
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	if manager.accountConcurrencyEnabled() {
		t.Fatal("account concurrency enabled while Home is active")
	}
}

func TestManagerAccountConcurrencyDisableWakesExistingWaiter(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(2 * time.Second))
	executor := &accountConcurrencyTestExecutor{entered: make(chan string, 2), release: make(chan struct{}, 2)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 2*time.Second)
	results := make(chan error, 2)
	run := func() {
		_, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		results <- errExecute
	}
	go run()
	_ = receiveString(t, executor.entered)
	go run()
	waitForManagerWaiters(t, manager, "account-a", 1)
	manager.SetConfig(&internalconfig.Config{})
	_ = receiveString(t, executor.entered)
	executor.release <- struct{}{}
	executor.release <- struct{}{}
	if errFirst := receiveError(t, results); errFirst != nil {
		t.Fatalf("first Execute() error = %v", errFirst)
	}
	if errSecond := receiveError(t, results); errSecond != nil {
		t.Fatalf("second Execute() error = %v", errSecond)
	}
}

func TestManagerAccountConcurrencyLimitIncreaseWakesWaiter(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(2 * time.Second))
	executor := &accountConcurrencyTestExecutor{entered: make(chan string, 2), release: make(chan struct{}, 2)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 2*time.Second)
	results := make(chan error, 2)
	run := func() {
		_, errExecute := manager.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		results <- errExecute
	}
	go run()
	_ = receiveString(t, executor.entered)
	go run()
	waitForManagerWaiters(t, manager, "account-a", 1)
	updated, _ := manager.GetByID("account-a")
	updated.Attributes[AttributeAccountMaxConcurrency] = "2"
	if _, errUpdate := manager.Update(context.Background(), updated); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	_ = receiveString(t, executor.entered)
	executor.release <- struct{}{}
	executor.release <- struct{}{}
	if errFirst := receiveError(t, results); errFirst != nil {
		t.Fatalf("first Execute() error = %v", errFirst)
	}
	if errSecond := receiveError(t, results); errSecond != nil {
		t.Fatalf("second Execute() error = %v", errSecond)
	}
}

func TestManagerAccountConcurrencyDisabledWhileWaitingReselects(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(time.Second))
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, time.Second)
	registerAccountConcurrencyTestAuth(t, manager, "account-b", "10", 1, 2, time.Second)
	occupied, acquired := manager.accountConcurrency.TryAcquire("account-a", 1)
	if !acquired {
		t.Fatal("failed to occupy account-a")
	}
	defer occupied.Release()

	type selectionResult struct {
		auth  *Auth
		lease accountconcurrency.Lease
		err   error
	}
	result := make(chan selectionResult, 1)
	go func() {
		selected, _, _, lease, errSelect := manager.pickNextMixedWithAccountAdmission(
			context.Background(), []string{"codex"}, "", cliproxyexecutor.Options{Metadata: map[string]any{
				cliproxyexecutor.PinnedAuthMetadataKey: "account-a",
			}}, map[string]struct{}{},
		)
		result <- selectionResult{auth: selected, lease: lease, err: errSelect}
	}()
	waitForManagerWaiters(t, manager, "account-a", 1)
	updated, _ := manager.GetByID("account-a")
	updated.Disabled = true
	updated.Status = StatusDisabled
	if _, errUpdate := manager.Update(context.Background(), updated); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}

	selected := <-result
	if selected.err != nil || selected.auth == nil || selected.auth.ID != "account-b" || selected.lease == nil {
		t.Fatalf("selection after disabling waiter account = %#v", selected)
	}
	selected.lease.Release()
}

func TestManagerAccountConcurrencySwitchesBeforeWaiting(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(time.Second))
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "20", 1, 1, time.Second)
	registerAccountConcurrencyTestAuth(t, manager, "account-b", "10", 1, 1, time.Second)
	lease, acquired := manager.accountConcurrency.TryAcquire("account-a", 1)
	if !acquired {
		t.Fatal("failed to occupy account-a")
	}
	defer lease.Release()

	auth, _, _, selectedLease, errSelect := manager.pickNextMixedWithAccountAdmission(
		context.Background(), []string{"codex"}, "", cliproxyexecutor.Options{}, map[string]struct{}{},
	)
	if errSelect != nil {
		t.Fatalf("pickNextMixedWithAccountAdmission() error = %v", errSelect)
	}
	if auth == nil || auth.ID != "account-b" {
		t.Fatalf("selected auth = %#v, want account-b", auth)
	}
	if selectedLease == nil {
		t.Fatal("selected lease = nil")
	}
	selectedLease.Release()
}

func TestManagerAccountConcurrencyZeroSwitchesStillProbesIdleAccounts(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	cfg := accountConcurrencyTestConfig(100 * time.Millisecond)
	cfg.AccountConcurrency.MaxAccountSwitches = 0
	manager.SetConfig(cfg)
	if got := manager.runtimeConfigSnapshot().AccountConcurrency.MaxAccountSwitches; got != 0 {
		t.Fatalf("runtime MaxAccountSwitches = %d, want explicit zero", got)
	}
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 1, 100*time.Millisecond)
	registerAccountConcurrencyTestAuth(t, manager, "account-b", "10", 1, 1, 100*time.Millisecond)
	occupied, acquired := manager.accountConcurrency.TryAcquire("account-a", 1)
	if !acquired {
		t.Fatal("failed to occupy account-a")
	}
	defer occupied.Release()

	upstreamAttempted := map[string]struct{}{}
	selected, _, _, lease, errSelect := manager.pickNextMixedWithAccountAdmission(
		context.Background(), []string{"codex"}, "", cliproxyexecutor.Options{}, upstreamAttempted,
	)
	if errSelect != nil {
		t.Fatalf("pickNextMixedWithAccountAdmission() error = %v", errSelect)
	}
	if selected == nil || selected.ID != "account-b" || lease == nil {
		t.Fatalf("selection = auth %#v, lease %#v; want idle account-b", selected, lease)
	}
	lease.Release()
	if len(upstreamAttempted) != 0 {
		t.Fatalf("capacity probing mutated upstream attempted set: %#v", upstreamAttempted)
	}
}

func TestManagerAccountConcurrencyWaitTimeoutIsTerminal(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(100 * time.Millisecond))
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 1, 100*time.Millisecond)
	lease, acquired := manager.accountConcurrency.TryAcquire("account-a", 1)
	if !acquired {
		t.Fatal("failed to occupy account-a")
	}
	defer lease.Release()

	_, _, _, selectedLease, errSelect := manager.pickNextMixedWithAccountAdmission(
		context.Background(), []string{"codex"}, "", cliproxyexecutor.Options{}, map[string]struct{}{},
	)
	if selectedLease != nil {
		t.Fatal("selected lease is non-nil after timeout")
	}
	var admissionErr *AdmissionError
	if !errors.As(errSelect, &admissionErr) {
		t.Fatalf("error = %T %v, want *AdmissionError", errSelect, errSelect)
	}
	if admissionErr.Code != AdmissionErrorWaitTimeout {
		t.Fatalf("admission code = %q, want %q", admissionErr.Code, AdmissionErrorWaitTimeout)
	}
	if _, retry := manager.shouldRetryAfterError(errSelect, 0, []string{"codex"}, "", time.Second); retry {
		t.Fatal("admission timeout entered outer retry loop")
	}
}

func TestAccountConcurrencyRequestStatePreservesBudgetAcrossAttempts(t *testing.T) {
	cfg := accountConcurrencyTestConfig(time.Second).AccountConcurrency
	ctx := withAccountConcurrencyRequestState(context.Background())
	first := accountConcurrencyStateFromContext(ctx, cfg)
	first.remainingWait -= 250 * time.Millisecond
	first.remainingSwitches--

	second := accountConcurrencyStateFromContext(ctx, cfg)
	if second != first {
		t.Fatal("request state was recreated")
	}
	if second.remainingWait != 750*time.Millisecond {
		t.Fatalf("remaining wait = %v, want 750ms", second.remainingWait)
	}
	if second.remainingSwitches != cfg.MaxAccountSwitches-1 {
		t.Fatalf("remaining switches = %d, want %d", second.remainingSwitches, cfg.MaxAccountSwitches-1)
	}
}

func TestManagerAccountConcurrencyStrictWaitPreservesSessionBinding(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	manager := NewManager(nil, selector, nil)
	manager.SetConfig(accountConcurrencyTestConfig(100 * time.Millisecond))
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 1, 100*time.Millisecond)
	registerAccountConcurrencyTestAuth(t, manager, "account-b", "10", 1, 1, 100*time.Millisecond)
	model := "gpt-account-concurrency-affinity"
	for _, authID := range []string{"account-a", "account-b"} {
		registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Session-ID": []string{"strict-wait"}}, Metadata: map[string]any{}}
	bound, _, _, errBind := manager.pickNextMixed(context.Background(), []string{"codex"}, model, opts, map[string]struct{}{})
	if errBind != nil {
		t.Fatalf("initial affinity selection error = %v", errBind)
	}
	if lookup, status := manager.LookupSessionAffinity("mixed", model, "strict-wait"); status != "bound" || lookup == nil || lookup.ID != bound.ID {
		t.Fatalf("affinity lookup = (%#v, %q), want bound %s", lookup, status, bound.ID)
	}
	occupied, acquired := manager.accountConcurrency.TryAcquire(bound.ID, 1)
	if !acquired {
		t.Fatalf("failed to occupy bound auth %s", bound.ID)
	}
	defer occupied.Release()

	selected, _, _, lease, errSelect := manager.pickNextMixedWithAccountAdmission(context.Background(), []string{"codex"}, model, opts, map[string]struct{}{})
	if selected != nil || lease != nil {
		t.Fatalf("strict wait selected another auth: selected=%#v lease=%#v", selected, lease)
	}
	var admissionErr *AdmissionError
	if !errors.As(errSelect, &admissionErr) || admissionErr.Code != AdmissionErrorWaitTimeout {
		t.Fatalf("strict wait error = %v, want wait timeout", errSelect)
	}
	otherID := "account-a"
	if bound.ID == otherID {
		otherID = "account-b"
	}
	if got := manager.accountConcurrency.Snapshot(otherID).Active; got != 0 {
		t.Fatalf("non-bound auth active = %d, want 0", got)
	}
}

func TestManagerAccountConcurrencyWaitThenSwitchWaitsBoundAccountFirst(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	manager := NewManager(nil, selector, nil)
	cfg := accountConcurrencyTestConfig(500 * time.Millisecond)
	cfg.Routing.SessionAffinityCapacityPolicy = "wait-then-switch"
	manager.SetConfig(cfg)
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 1, 100*time.Millisecond)
	registerAccountConcurrencyTestAuth(t, manager, "account-b", "10", 1, 1, 100*time.Millisecond)
	model := "gpt-account-concurrency-switch-affinity"
	for _, authID := range []string{"account-a", "account-b"} {
		registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Session-ID": []string{"wait-then-switch"}}, Metadata: map[string]any{}}
	bound, _, _, errBind := manager.pickNextMixed(context.Background(), []string{"codex"}, model, opts, map[string]struct{}{})
	if errBind != nil {
		t.Fatalf("initial affinity selection error = %v", errBind)
	}
	occupied, acquired := manager.accountConcurrency.TryAcquire(bound.ID, 1)
	if !acquired {
		t.Fatalf("failed to occupy bound auth %s", bound.ID)
	}
	defer occupied.Release()

	type selectionResult struct {
		auth  *Auth
		lease accountconcurrency.Lease
		err   error
	}
	result := make(chan selectionResult, 1)
	go func() {
		selected, _, _, lease, errSelect := manager.pickNextMixedWithAccountAdmission(context.Background(), []string{"codex"}, model, opts, map[string]struct{}{})
		result <- selectionResult{auth: selected, lease: lease, err: errSelect}
	}()
	waitForManagerWaiters(t, manager, bound.ID, 1)
	selected := <-result
	if selected.err != nil {
		t.Fatalf("wait-then-switch selection error = %v", selected.err)
	}
	if selected.auth == nil || selected.auth.ID == bound.ID || selected.lease == nil {
		t.Fatalf("wait-then-switch selection = %#v", selected)
	}
	selected.lease.Release()
}

func TestManagerAccountConcurrencyWaitThenSwitchCommitsAffinityAfterLease(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	manager := NewManager(nil, selector, nil)
	cfg := accountConcurrencyTestConfig(2 * time.Second)
	cfg.Routing.SessionAffinityCapacityPolicy = "wait-then-switch"
	manager.SetConfig(cfg)
	manager.RegisterExecutor(&accountConcurrencyTestExecutor{entered: make(chan string, 1), release: make(chan struct{}, 1)})
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 100*time.Millisecond)
	registerAccountConcurrencyTestAuth(t, manager, "account-b", "10", 1, 2, time.Second)
	model := "gpt-account-concurrency-deferred-affinity"
	for _, authID := range []string{"account-a", "account-b"} {
		registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Session-ID": []string{"deferred-switch"}}, Metadata: map[string]any{}}
	bound, _, _, errBind := manager.pickNextMixed(context.Background(), []string{"codex"}, model, opts, map[string]struct{}{})
	if errBind != nil {
		t.Fatalf("initial affinity selection error = %v", errBind)
	}
	otherID := "account-a"
	if bound.ID == otherID {
		otherID = "account-b"
	}
	boundLease, acquiredBound := manager.accountConcurrency.TryAcquire(bound.ID, 1)
	if !acquiredBound {
		t.Fatalf("failed to occupy bound auth %s", bound.ID)
	}
	defer boundLease.Release()
	otherLease, acquiredOther := manager.accountConcurrency.TryAcquire(otherID, 1)
	if !acquiredOther {
		t.Fatalf("failed to occupy alternate auth %s", otherID)
	}
	defer otherLease.Release()

	type selectionResult struct {
		auth  *Auth
		lease accountconcurrency.Lease
		err   error
	}
	result := make(chan selectionResult, 1)
	go func() {
		selected, _, _, lease, errSelect := manager.pickNextMixedWithAccountAdmission(context.Background(), []string{"codex"}, model, opts, map[string]struct{}{})
		result <- selectionResult{auth: selected, lease: lease, err: errSelect}
	}()
	waitForManagerWaiters(t, manager, otherID, 1)
	if lookup, status := manager.LookupSessionAffinity("mixed", model, "deferred-switch"); status != "bound" || lookup == nil || lookup.ID != bound.ID {
		t.Fatalf("affinity changed before alternate lease: lookup=%#v status=%q, want %s", lookup, status, bound.ID)
	}
	otherLease.Release()
	selected := <-result
	if selected.err != nil || selected.auth == nil || selected.auth.ID != otherID || selected.lease == nil {
		t.Fatalf("selection after alternate release = %#v", selected)
	}
	if lookup, status := manager.LookupSessionAffinity("mixed", model, "deferred-switch"); status != "bound" || lookup == nil || lookup.ID != otherID {
		t.Fatalf("affinity after alternate lease: lookup=%#v status=%q, want %s", lookup, status, otherID)
	}
	selected.lease.Release()
}

func accountConcurrencyTestConfig(maxWait time.Duration) *internalconfig.Config {
	return &internalconfig.Config{AccountConcurrency: internalconfig.AccountConcurrencyConfig{
		Enabled:            true,
		MaxTotalWait:       maxWait.String(),
		MaxAccountSwitches: 2,
		MaxTotalWaiters:    100,
		Store:              "memory",
	}}
}

func registerAccountConcurrencyTestAuth(t *testing.T, manager *Manager, id, priority string, maxConcurrency, maxWaiting int, waitTimeout time.Duration) {
	t.Helper()
	_, errRegister := manager.Register(context.Background(), &Auth{
		ID:       id,
		Provider: "codex",
		Status:   StatusActive,
		Attributes: map[string]string{
			"auth_kind":                    "oauth",
			"priority":                     priority,
			AttributeAccountMaxConcurrency: integerString(maxConcurrency),
			AttributeAccountMaxWaiting:     integerString(maxWaiting),
			AttributeAccountWaitTimeoutMS:  integerString(int(waitTimeout / time.Millisecond)),
		},
	})
	if errRegister != nil {
		t.Fatalf("Register(%s) error = %v", id, errRegister)
	}
}

func integerString(value int) string {
	if value == 0 {
		return "0"
	}
	result := make([]byte, 0, 12)
	for value > 0 {
		result = append(result, byte('0'+value%10))
		value /= 10
	}
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return string(result)
}

func waitForManagerWaiters(t *testing.T, manager *Manager, accountID string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for manager.accountConcurrency.Snapshot(accountID).Waiting != count {
		if time.Now().After(deadline) {
			t.Fatalf("waiters did not reach %d; snapshot=%#v", count, manager.accountConcurrency.Snapshot(accountID))
		}
		runtime.Gosched()
	}
}

func receiveString(t *testing.T, values <-chan string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for string")
		return ""
	}
}

func receiveError(t *testing.T, values <-chan error) error {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
		return nil
	}
}
