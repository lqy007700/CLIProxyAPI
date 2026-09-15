package auth

import (
	"testing"
	"time"
)

func TestApplyAccountConcurrencyMetadata(t *testing.T) {
	auth := &Auth{
		Provider:   "codex",
		Attributes: map[string]string{"auth_kind": "oauth"},
		Metadata: map[string]any{
			"max_concurrency": float64(3),
			"max_waiting":     float64(7),
			"wait_timeout_ms": float64(1250),
		},
	}

	if err := ApplyAccountConcurrencyMetadata(auth); err != nil {
		t.Fatalf("ApplyAccountConcurrencyMetadata() error = %v", err)
	}
	if got := auth.Attributes[AttributeAccountMaxConcurrency]; got != "3" {
		t.Fatalf("max concurrency attribute = %q, want 3", got)
	}
	if got := auth.Attributes[AttributeAccountMaxWaiting]; got != "7" {
		t.Fatalf("max waiting attribute = %q, want 7", got)
	}
	if got := auth.Attributes[AttributeAccountWaitTimeoutMS]; got != "1250" {
		t.Fatalf("wait timeout attribute = %q, want 1250", got)
	}

	limits, ok := AccountConcurrencyLimitsFromAuth(auth)
	if !ok {
		t.Fatal("AccountConcurrencyLimitsFromAuth() ok = false, want true")
	}
	if limits.MaxConcurrency != 3 || limits.MaxWaiting != 7 || limits.WaitTimeout != 1250*time.Millisecond {
		t.Fatalf("limits = %#v", limits)
	}
}

func TestApplyAccountConcurrencyMetadataClearsRemovedValues(t *testing.T) {
	auth := &Auth{
		Attributes: map[string]string{
			AttributeAccountMaxConcurrency: "3",
			AttributeAccountMaxWaiting:     "7",
			AttributeAccountWaitTimeoutMS:  "1250",
		},
	}

	if err := ApplyAccountConcurrencyMetadata(auth); err != nil {
		t.Fatalf("ApplyAccountConcurrencyMetadata() error = %v", err)
	}
	for _, key := range []string{
		AttributeAccountMaxConcurrency,
		AttributeAccountMaxWaiting,
		AttributeAccountWaitTimeoutMS,
	} {
		if _, exists := auth.Attributes[key]; exists {
			t.Fatalf("attribute %q still exists after metadata removal", key)
		}
	}
}

func TestAccountConcurrencyLimitsFromAuthScope(t *testing.T) {
	tests := []struct {
		name string
		auth *Auth
		want bool
	}{
		{name: "codex oauth", auth: &Auth{Provider: "codex", Attributes: map[string]string{"auth_kind": "oauth", AttributeAccountMaxConcurrency: "1"}}, want: true},
		{name: "codex api key", auth: &Auth{Provider: "codex", Attributes: map[string]string{"auth_kind": "apikey", AttributeAccountMaxConcurrency: "1"}}, want: false},
		{name: "claude oauth", auth: &Auth{Provider: "claude", Attributes: map[string]string{"auth_kind": "oauth", AttributeAccountMaxConcurrency: "1"}}, want: false},
		{name: "plugin virtual", auth: &Auth{Provider: "codex", Attributes: map[string]string{"auth_kind": "oauth", AttributePluginVirtual: "true", AttributeAccountMaxConcurrency: "1"}}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := AccountConcurrencyLimitsFromAuth(tc.auth)
			if got != tc.want {
				t.Fatalf("ok = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestApplyAccountConcurrencyMetadataRejectsInvalidValues(t *testing.T) {
	tests := []map[string]any{
		{"max_concurrency": float64(-1)},
		{"max_concurrency": 1.5},
		{"max_concurrency": "3"},
		{"max_concurrency": float64(10_001)},
		{"max_waiting": "many"},
		{"max_concurrency": float64(1), "max_waiting": float64(10_001)},
		{"wait_timeout_ms": float64(-1)},
		{"max_concurrency": float64(0), "max_waiting": float64(1)},
		{"max_concurrency": float64(1), "max_waiting": float64(0), "wait_timeout_ms": float64(100)},
		{"max_concurrency": float64(1), "max_waiting": float64(1), "wait_timeout_ms": float64(99)},
		{"max_concurrency": float64(1), "max_waiting": float64(1), "wait_timeout_ms": float64(300_001)},
	}
	for _, metadata := range tests {
		if err := ApplyAccountConcurrencyMetadata(&Auth{Metadata: metadata}); err == nil {
			t.Fatalf("ApplyAccountConcurrencyMetadata(%v) error = nil", metadata)
		}
	}
}
