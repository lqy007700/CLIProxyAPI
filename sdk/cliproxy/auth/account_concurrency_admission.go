package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	accountconcurrency "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth/concurrency"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	cliproxysession "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/session"
)

const (
	AdmissionErrorAccountQueueFull = "account_concurrency_queue_full"
	AdmissionErrorGlobalQueueFull  = "account_concurrency_global_queue_full"
	AdmissionErrorWaitTimeout      = "account_concurrency_wait_timeout"
)

// AdmissionError is a local capacity failure that must not enter provider retry or cooldown logic.
type AdmissionError struct {
	Code       string
	Message    string
	HTTPStatus int
	Cause      error
}

func (e *AdmissionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return e.Code
	}
	return e.Code + ": " + e.Message
}

func (e *AdmissionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *AdmissionError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.HTTPStatus
}

func isAccountConcurrencyAdmissionError(err error) bool {
	var admissionErr *AdmissionError
	return errors.As(err, &admissionErr) && admissionErr != nil
}

type accountAdmissionCandidate struct {
	auth                  *Auth
	executor              ProviderExecutor
	provider              string
	limits                AccountConcurrencyLimits
	commitAffinityBinding sessionAffinityBindingCommit
}

type accountConcurrencyRequestStateKey struct{}

type accountConcurrencyRequestState struct {
	initialized       bool
	remainingWait     time.Duration
	remainingSwitches int
}

func withAccountConcurrencyRequestState(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(accountConcurrencyRequestStateKey{}).(*accountConcurrencyRequestState); ok {
		return ctx
	}
	return context.WithValue(ctx, accountConcurrencyRequestStateKey{}, &accountConcurrencyRequestState{})
}

func accountConcurrencyStateFromContext(ctx context.Context, cfg internalconfig.AccountConcurrencyConfig) *accountConcurrencyRequestState {
	var state *accountConcurrencyRequestState
	if ctx != nil {
		state, _ = ctx.Value(accountConcurrencyRequestStateKey{}).(*accountConcurrencyRequestState)
	}
	if state == nil {
		state = &accountConcurrencyRequestState{}
	}
	if !state.initialized {
		state.initialized = true
		state.remainingWait = cfg.MaxTotalWaitDuration()
		state.remainingSwitches = cfg.MaxAccountSwitches
	}
	return state
}

func (candidate *accountAdmissionCandidate) commitAffinity() {
	if candidate != nil && candidate.commitAffinityBinding != nil {
		candidate.commitAffinityBinding()
		candidate.commitAffinityBinding = nil
	}
}

func (m *Manager) pickNextMixedWithAccountAdmission(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}) (*Auth, ProviderExecutor, string, accountconcurrency.Lease, error) {
	if !m.accountConcurrencyEnabled() {
		auth, executor, provider, errPick := m.pickNextMixed(ctx, providers, model, opts, tried)
		return auth, executor, provider, nil, errPick
	}

	runtimeCfg := m.runtimeConfigSnapshot()
	cfg := runtimeCfg.AccountConcurrency
	requestState := accountConcurrencyStateFromContext(ctx, cfg)
	pinned := pinnedAuthIDFromMetadata(opts.Metadata) != ""
	bound := m.accountConcurrencyBoundSessionAuth(model, opts) != ""
	policy := accountConcurrencyCapacityPolicy(runtimeCfg)
	remainingWait := requestState.remainingWait
	remainingSwitches := requestState.remainingSwitches
	defer func() {
		requestState.remainingWait = remainingWait
		requestState.remainingSwitches = remainingSwitches
	}()
	selectionExcluded := cloneStringSet(tried)
	candidates := make([]accountAdmissionCandidate, 0)
	var lastPickErr error
	var firstFullAccountID string

	// An existing affinity binding is authoritative. strict-wait never probes another
	// account; wait-then-switch waits once before clearing the request pin/exclusion.
	if pinned || bound {
		candidate, lease, limited, errSelect := m.probeAccountConcurrencyCandidate(ctx, providers, model, opts, selectionExcluded)
		if errSelect != nil || !limited || lease != nil {
			if candidate == nil {
				return nil, nil, "", nil, errSelect
			}
			candidate.commitAffinity()
			return candidate.auth, candidate.executor, candidate.provider, lease, errSelect
		}
		selectionExcluded[candidate.auth.ID] = struct{}{}
		waitLease, errWait, waited := m.waitForAccountConcurrencyCandidate(ctx, candidate, remainingWait)
		remainingWait -= waited
		if errWait == nil {
			candidate.commitAffinity()
			return candidate.auth, candidate.executor, candidate.provider, waitLease, nil
		}
		accountUnavailable := errors.Is(errWait, accountconcurrency.ErrAccountUnavailable)
		if !accountUnavailable {
			if policy == "strict-wait" || remainingSwitches == 0 || isTerminalAccountConcurrencyWaitError(errWait) {
				return nil, nil, "", nil, errWait
			}
			remainingSwitches--
		}
		lastPickErr = errWait
		opts.EnsureMetadata()
		delete(opts.Metadata, cliproxyexecutor.PinnedAuthMetadataKey)
	}

	// Capacity probing does not consume account-switch budget. Probe every eligible
	// candidate before selecting a queue target so an idle account always wins.
	for {
		candidate, lease, limited, errSelect := m.probeAccountConcurrencyCandidate(ctx, providers, model, opts, selectionExcluded)
		if errSelect != nil {
			lastPickErr = errSelect
			break
		}
		if !limited {
			candidate.commitAffinity()
			return candidate.auth, candidate.executor, candidate.provider, nil, nil
		}
		if lease != nil {
			candidate.commitAffinity()
			return candidate.auth, candidate.executor, candidate.provider, lease, nil
		}
		if firstFullAccountID == "" {
			firstFullAccountID = candidate.auth.ID
		}
		if candidate.limits.MaxWaiting > 0 {
			candidates = append(candidates, *candidate)
		}
		selectionExcluded[candidate.auth.ID] = struct{}{}
	}
	if len(candidates) == 0 {
		if firstFullAccountID != "" {
			return nil, nil, "", nil, newAdmissionError(AdmissionErrorAccountQueueFull, firstFullAccountID, accountconcurrency.ErrAccountQueueFull)
		}
		return nil, nil, "", nil, lastPickErr
	}

	var lastAdmissionErr error
	for _, candidate := range candidates {
		lease, errAcquire, waited := m.waitForAccountConcurrencyCandidate(ctx, &candidate, remainingWait)
		remainingWait -= waited
		if errAcquire == nil {
			candidate.commitAffinity()
			return candidate.auth, candidate.executor, candidate.provider, lease, nil
		}
		lastAdmissionErr = errAcquire
		if isTerminalAccountConcurrencyWaitError(errAcquire) {
			return nil, nil, "", nil, errAcquire
		}
		if errors.Is(errAcquire, accountconcurrency.ErrAccountUnavailable) {
			continue
		}
		if remainingSwitches == 0 {
			break
		}
		remainingSwitches--
	}
	if lastAdmissionErr == nil {
		lastAdmissionErr = newAdmissionError(AdmissionErrorAccountQueueFull, candidates[0].auth.ID, accountconcurrency.ErrAccountQueueFull)
	}
	return nil, nil, "", nil, lastAdmissionErr
}

func (m *Manager) probeAccountConcurrencyCandidate(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}) (*accountAdmissionCandidate, accountconcurrency.Lease, bool, error) {
	opts.EnsureMetadata()
	opts.Metadata[deferSessionAffinityBindingMetadataKey] = true
	delete(opts.Metadata, pendingSessionAffinityBindingMetadataKey)
	auth, executor, provider, errPick := m.pickNextMixed(ctx, providers, model, opts, tried)
	commitAffinity := takePendingSessionAffinityBinding(opts.Metadata)
	if errPick != nil {
		return nil, nil, false, errPick
	}
	candidate := &accountAdmissionCandidate{auth: auth, executor: executor, provider: provider, commitAffinityBinding: commitAffinity}
	limits, limited := AccountConcurrencyLimitsFromAuth(auth)
	if !limited {
		return candidate, nil, false, nil
	}
	candidate.limits = limits
	if lease, acquired := m.accountConcurrency.TryAcquire(auth.ID, limits.MaxConcurrency); acquired {
		logEntryWithRequestID(ctx).WithField("auth_id", auth.ID).Debug("account concurrency lease acquired")
		return candidate, lease, true, nil
	}
	logEntryWithRequestID(ctx).WithField("auth_id", auth.ID).Debug("account concurrency capacity probe is full")
	return candidate, nil, true, nil
}

func (m *Manager) waitForAccountConcurrencyCandidate(ctx context.Context, candidate *accountAdmissionCandidate, budget time.Duration) (accountconcurrency.Lease, error, time.Duration) {
	if candidate == nil || candidate.auth == nil || budget <= 0 {
		accountID := ""
		if candidate != nil && candidate.auth != nil {
			accountID = candidate.auth.ID
		}
		return nil, newAdmissionError(AdmissionErrorWaitTimeout, accountID, context.DeadlineExceeded), 0
	}
	waitFor := candidate.limits.WaitTimeout
	if waitFor <= 0 || waitFor > budget {
		waitFor = budget
	}
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	waitCtx, cancelWait := context.WithTimeout(baseCtx, waitFor)
	logEntryWithRequestID(ctx).WithFields(map[string]any{
		"auth_id":    candidate.auth.ID,
		"wait_limit": waitFor,
	}).Debug("account concurrency wait started")
	waitStarted := time.Now()
	lease, errAcquire := m.accountConcurrency.Acquire(waitCtx, candidate.auth.ID, accountconcurrency.Limits{
		MaxActive:  candidate.limits.MaxConcurrency,
		MaxWaiting: candidate.limits.MaxWaiting,
	})
	waited := time.Since(waitStarted)
	cancelWait()
	if errAcquire == nil {
		logEntryWithRequestID(ctx).WithField("auth_id", candidate.auth.ID).Debug("account concurrency waiter acquired lease")
		return lease, nil, waited
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err(), waited
	}
	switch {
	case errors.Is(errAcquire, accountconcurrency.ErrAccountUnavailable):
		return nil, accountconcurrency.ErrAccountUnavailable, waited
	case errors.Is(errAcquire, accountconcurrency.ErrAccountQueueFull):
		return nil, newAdmissionError(AdmissionErrorAccountQueueFull, candidate.auth.ID, errAcquire), waited
	case errors.Is(errAcquire, accountconcurrency.ErrGlobalQueueFull):
		return nil, newAdmissionError(AdmissionErrorGlobalQueueFull, candidate.auth.ID, errAcquire), waited
	default:
		return nil, newAdmissionError(AdmissionErrorWaitTimeout, candidate.auth.ID, errAcquire), waited
	}
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	cloned := make(map[string]struct{}, len(source))
	for key := range source {
		cloned[key] = struct{}{}
	}
	return cloned
}

func isTerminalAccountConcurrencyWaitError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, accountconcurrency.ErrGlobalQueueFull) {
		return true
	}
	var admissionErr *AdmissionError
	if errors.As(err, &admissionErr) {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (m *Manager) accountConcurrencyBoundSessionAuth(model string, opts cliproxyexecutor.Options) string {
	if m == nil {
		return ""
	}
	sessionID := ""
	if rawSessionID, ok := opts.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey]; ok {
		sessionID, _ = rawSessionID.(string)
	}
	if strings.TrimSpace(sessionID) == "" {
		if info, ok := cliproxysession.ExtractSessionInfo(opts.Headers, opts.OriginalRequest, opts.Metadata); ok {
			sessionID = info.SessionID
		}
	}
	if strings.TrimSpace(sessionID) == "" {
		return ""
	}
	auth, status := m.LookupSessionAffinity("mixed", model, sessionID)
	if status != "bound" || auth == nil {
		return ""
	}
	return auth.ID
}

func (m *Manager) accountConcurrencyEnabled() bool {
	if m == nil || m.accountConcurrency == nil || m.HomeEnabled() || m.hasPluginScheduler() {
		return false
	}
	cfg := m.runtimeConfigSnapshot()
	return cfg != nil && cfg.AccountConcurrency.Enabled
}

// AccountConcurrencySnapshot returns the current local admission counters for one auth.
func (m *Manager) AccountConcurrencySnapshot(authID string) accountconcurrency.Snapshot {
	if m == nil || m.accountConcurrency == nil {
		return accountconcurrency.Snapshot{}
	}
	snapshot := m.accountConcurrency.Snapshot(authID)
	if snapshot.Limit == 0 {
		if auth, ok := m.GetByID(authID); ok {
			if limits, limited := AccountConcurrencyLimitsFromAuth(auth); limited {
				snapshot.Limit = limits.MaxConcurrency
			}
		}
	}
	return snapshot
}

func (m *Manager) syncAccountConcurrencyAuth(auth *Auth) {
	if m == nil || m.accountConcurrency == nil || auth == nil {
		return
	}
	if auth.Disabled || auth.Status == StatusDisabled {
		m.accountConcurrency.CancelAccount(auth.ID)
		return
	}
	if limits, limited := AccountConcurrencyLimitsFromAuth(auth); limited {
		m.accountConcurrency.UpdateLimit(auth.ID, limits.MaxConcurrency)
		return
	}
	m.accountConcurrency.BypassAccount(auth.ID)
}

func accountConcurrencyCapacityPolicy(cfg *internalconfig.Config) string {
	if cfg == nil {
		return "strict-wait"
	}
	policy := strings.ToLower(strings.TrimSpace(cfg.Routing.SessionAffinityCapacityPolicy))
	if policy == "" {
		return "strict-wait"
	}
	return policy
}

func newAdmissionError(code, accountID string, cause error) *AdmissionError {
	return &AdmissionError{
		Code:       code,
		Message:    fmt.Sprintf("account %s has no available concurrency slot", accountID),
		HTTPStatus: http.StatusTooManyRequests,
		Cause:      cause,
	}
}

func wrapStreamResultWithAccountLease(ctx context.Context, result *cliproxyexecutor.StreamResult, lease accountconcurrency.Lease) *cliproxyexecutor.StreamResult {
	if lease == nil {
		return result
	}
	if result == nil || result.Chunks == nil {
		lease.Release()
		return result
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer lease.Release()
		for {
			var (
				chunk cliproxyexecutor.StreamChunk
				ok    bool
			)
			if ctx == nil {
				chunk, ok = <-result.Chunks
			} else {
				select {
				case <-ctx.Done():
					discardStreamChunks(result.Chunks)
					return
				case chunk, ok = <-result.Chunks:
				}
			}
			if !ok {
				return
			}
			if ctx == nil {
				out <- chunk
				continue
			}
			select {
			case <-ctx.Done():
				discardStreamChunks(result.Chunks)
				return
			case out <- chunk:
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}
}
