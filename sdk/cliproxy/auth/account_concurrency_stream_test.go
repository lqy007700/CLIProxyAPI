package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type accountConcurrencyStreamExecutor struct {
	created chan chan cliproxyexecutor.StreamChunk
}

func (e *accountConcurrencyStreamExecutor) Identifier() string { return "codex" }
func (e *accountConcurrencyStreamExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}
func (e *accountConcurrencyStreamExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data: started\n\n")}
	e.created <- chunks
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}
func (e *accountConcurrencyStreamExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}
func (e *accountConcurrencyStreamExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}
func (e *accountConcurrencyStreamExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestManagerAccountConcurrencyStreamLeaseHeldUntilEOF(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(2 * time.Second))
	executor := &accountConcurrencyStreamExecutor{created: make(chan chan cliproxyexecutor.StreamChunk, 2)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 2, 2*time.Second)

	first, errFirst := manager.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{Stream: true})
	if errFirst != nil {
		t.Fatalf("first ExecuteStream() error = %v", errFirst)
	}
	firstUpstream := receiveChunkChannel(t, executor.created)
	if got := manager.accountConcurrency.Snapshot("account-a").Active; got != 1 {
		t.Fatalf("active after stream start = %d, want 1", got)
	}

	secondResult := make(chan *cliproxyexecutor.StreamResult, 1)
	secondErr := make(chan error, 1)
	go func() {
		stream, errStream := manager.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{Stream: true})
		secondResult <- stream
		secondErr <- errStream
	}()
	waitForManagerWaiters(t, manager, "account-a", 1)
	close(firstUpstream)
	drainChunks(first.Chunks)

	second := receiveStreamResult(t, secondResult)
	if errStream := receiveError(t, secondErr); errStream != nil {
		t.Fatalf("second ExecuteStream() error = %v", errStream)
	}
	secondUpstream := receiveChunkChannel(t, executor.created)
	close(secondUpstream)
	drainChunks(second.Chunks)
	waitForManagerActive(t, manager, "account-a", 0)
}

func TestManagerAccountConcurrencyStreamCancellationReleasesLease(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(accountConcurrencyTestConfig(time.Second))
	executor := &accountConcurrencyStreamExecutor{created: make(chan chan cliproxyexecutor.StreamChunk, 1)}
	manager.RegisterExecutor(executor)
	registerAccountConcurrencyTestAuth(t, manager, "account-a", "10", 1, 1, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	stream, errStream := manager.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{Stream: true})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	upstream := receiveChunkChannel(t, executor.created)
	cancel()
	drainChunks(stream.Chunks)
	waitForManagerActive(t, manager, "account-a", 0)
	close(upstream)
}

func receiveChunkChannel(t *testing.T, values <-chan chan cliproxyexecutor.StreamChunk) chan cliproxyexecutor.StreamChunk {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream stream")
		return nil
	}
}

func receiveStreamResult(t *testing.T, values <-chan *cliproxyexecutor.StreamResult) *cliproxyexecutor.StreamResult {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream result")
		return nil
	}
}

func drainChunks(ch <-chan cliproxyexecutor.StreamChunk) {
	for range ch {
	}
}

func waitForManagerActive(t *testing.T, manager *Manager, accountID string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for manager.accountConcurrency.Snapshot(accountID).Active != count {
		if time.Now().After(deadline) {
			t.Fatalf("active count did not reach %d; snapshot=%#v", count, manager.accountConcurrency.Snapshot(accountID))
		}
	}
}
