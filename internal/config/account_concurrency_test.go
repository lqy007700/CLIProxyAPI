package config

import (
	"testing"
	"time"
)

func TestParseConfigBytesAccountConcurrencyDefaults(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte("port: 8317\n"))
	if err != nil {
		t.Fatalf("ParseConfigBytes() error = %v", err)
	}
	if cfg.AccountConcurrency.Enabled {
		t.Fatal("account concurrency must be disabled by default")
	}
	if got := cfg.AccountConcurrency.MaxTotalWaitDuration(); got != 30*time.Second {
		t.Fatalf("MaxTotalWaitDuration() = %v, want 30s", got)
	}
	if got := cfg.AccountConcurrency.MaxAccountSwitches; got != 2 {
		t.Fatalf("MaxAccountSwitches = %d, want 2", got)
	}
	if got := cfg.AccountConcurrency.MaxTotalWaiters; got != 10_000 {
		t.Fatalf("MaxTotalWaiters = %d, want 10000", got)
	}
	if got := cfg.AccountConcurrency.Store; got != "memory" {
		t.Fatalf("Store = %q, want memory", got)
	}
	if got := cfg.Routing.SessionAffinityCapacityPolicy; got != "strict-wait" {
		t.Fatalf("SessionAffinityCapacityPolicy = %q, want strict-wait", got)
	}
}

func TestParseConfigBytesAccountConcurrencyExplicitZeroSwitches(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(`
account-concurrency:
  enabled: true
  max-total-wait: 750ms
  max-account-switches: 0
  max-total-waiters: 12
  store: memory
`))
	if err != nil {
		t.Fatalf("ParseConfigBytes() error = %v", err)
	}
	if !cfg.AccountConcurrency.Enabled {
		t.Fatal("Enabled = false, want true")
	}
	if got := cfg.AccountConcurrency.MaxTotalWaitDuration(); got != 750*time.Millisecond {
		t.Fatalf("MaxTotalWaitDuration() = %v, want 750ms", got)
	}
	if got := cfg.AccountConcurrency.MaxAccountSwitches; got != 0 {
		t.Fatalf("MaxAccountSwitches = %d, want 0", got)
	}
}

func TestParseConfigBytesRejectsInvalidAccountConcurrency(t *testing.T) {
	tests := []string{
		"max-total-wait: nope",
		"max-total-wait: 99ms",
		"max-total-wait: 5m1s",
		"max-account-switches: -1",
		"max-account-switches: 101",
		"max-total-waiters: -1",
		"max-total-waiters: 100001",
		"store: redis",
	}
	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			_, err := ParseConfigBytes([]byte("account-concurrency:\n  enabled: true\n  " + body + "\n"))
			if err == nil {
				t.Fatal("ParseConfigBytes() error = nil, want validation error")
			}
		})
	}
}

func TestParseConfigBytesRejectsInvalidSessionAffinityCapacityPolicy(t *testing.T) {
	_, err := ParseConfigBytes([]byte("routing:\n  session-affinity-capacity-policy: switch-now\n"))
	if err == nil {
		t.Fatal("ParseConfigBytes() error = nil, want validation error")
	}
}
