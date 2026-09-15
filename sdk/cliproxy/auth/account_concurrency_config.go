package auth

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	AttributeAccountMaxConcurrency = "account_concurrency.max_concurrency"
	AttributeAccountMaxWaiting     = "account_concurrency.max_waiting"
	AttributeAccountWaitTimeoutMS  = "account_concurrency.wait_timeout_ms"
)

// AccountConcurrencyLimits are immutable admission settings derived from an auth file.
type AccountConcurrencyLimits struct {
	MaxConcurrency int
	MaxWaiting     int
	WaitTimeout    time.Duration
}

// ApplyAccountConcurrencyMetadata copies supported numeric auth-file settings into attributes.
func ApplyAccountConcurrencyMetadata(auth *Auth) error {
	if auth == nil {
		return nil
	}
	fields := []struct {
		metadataKey  string
		attributeKey string
	}{
		{metadataKey: "max_concurrency", attributeKey: AttributeAccountMaxConcurrency},
		{metadataKey: "max_waiting", attributeKey: AttributeAccountMaxWaiting},
		{metadataKey: "wait_timeout_ms", attributeKey: AttributeAccountWaitTimeoutMS},
	}
	values := make(map[string]int, len(fields))
	for _, field := range fields {
		raw, exists := auth.Metadata[field.metadataKey]
		if !exists || raw == nil {
			values[field.metadataKey] = 0
			continue
		}
		value, errValue := accountConcurrencyInt(raw)
		if errValue != nil {
			return fmt.Errorf("%s: %w", field.metadataKey, errValue)
		}
		values[field.metadataKey] = value
	}
	maxConcurrency := values["max_concurrency"]
	maxWaiting := values["max_waiting"]
	waitTimeoutMS := values["wait_timeout_ms"]
	if maxConcurrency > 10_000 {
		return fmt.Errorf("max_concurrency: must be between 0 and 10000")
	}
	if maxWaiting > 10_000 {
		return fmt.Errorf("max_waiting: must be between 0 and 10000")
	}
	if waitTimeoutMS != 0 && (waitTimeoutMS < 100 || waitTimeoutMS > 300_000) {
		return fmt.Errorf("wait_timeout_ms: must be 0 or between 100 and 300000")
	}
	if maxConcurrency == 0 && (maxWaiting != 0 || waitTimeoutMS != 0) {
		return fmt.Errorf("max_waiting and wait_timeout_ms must be 0 when max_concurrency is 0")
	}
	if maxConcurrency > 0 && maxWaiting == 0 && waitTimeoutMS != 0 {
		return fmt.Errorf("wait_timeout_ms must be 0 when max_waiting is 0")
	}
	if maxConcurrency > 0 && maxWaiting > 0 && waitTimeoutMS < 100 {
		return fmt.Errorf("wait_timeout_ms must be at least 100 when max_waiting is positive")
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	for _, field := range fields {
		value := values[field.metadataKey]
		if value == 0 {
			delete(auth.Attributes, field.attributeKey)
			continue
		}
		auth.Attributes[field.attributeKey] = strconv.Itoa(value)
	}
	return nil
}

// AccountConcurrencyLimitsFromAuth returns limits only for first-version Codex OAuth accounts.
func AccountConcurrencyLimitsFromAuth(auth *Auth) (AccountConcurrencyLimits, bool) {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") || IsPluginVirtualAuth(auth) {
		return AccountConcurrencyLimits{}, false
	}
	if auth.AuthKind() != AuthKindOAuth {
		return AccountConcurrencyLimits{}, false
	}
	maxConcurrency, ok := accountConcurrencyAttribute(auth, AttributeAccountMaxConcurrency)
	if !ok || maxConcurrency <= 0 {
		return AccountConcurrencyLimits{}, false
	}
	maxWaiting, _ := accountConcurrencyAttribute(auth, AttributeAccountMaxWaiting)
	waitTimeoutMS, _ := accountConcurrencyAttribute(auth, AttributeAccountWaitTimeoutMS)
	return AccountConcurrencyLimits{
		MaxConcurrency: maxConcurrency,
		MaxWaiting:     maxWaiting,
		WaitTimeout:    time.Duration(waitTimeoutMS) * time.Millisecond,
	}, true
}

func accountConcurrencyAttribute(auth *Auth, key string) (int, bool) {
	if auth == nil || len(auth.Attributes) == 0 {
		return 0, false
	}
	raw := strings.TrimSpace(auth.Attributes[key])
	if raw == "" {
		return 0, false
	}
	value, errValue := strconv.Atoi(raw)
	if errValue != nil || value < 0 {
		return 0, false
	}
	return value, true
}

func accountConcurrencyInt(raw any) (int, error) {
	var value int64
	switch typed := raw.(type) {
	case int:
		value = int64(typed)
	case int8:
		value = int64(typed)
	case int16:
		value = int64(typed)
	case int32:
		value = int64(typed)
	case int64:
		value = typed
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, fmt.Errorf("must be a non-negative integer")
		}
		value = int64(typed)
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("must be a non-negative integer")
		}
		value = int64(typed)
	case float64:
		if math.Trunc(typed) != typed || typed > math.MaxInt64 {
			return 0, fmt.Errorf("must be a non-negative integer")
		}
		value = int64(typed)
	case json.Number:
		parsed, errParse := strconv.ParseInt(string(typed), 10, 64)
		if errParse != nil {
			return 0, fmt.Errorf("must be a non-negative integer")
		}
		value = parsed
	default:
		return 0, fmt.Errorf("must be a non-negative integer")
	}
	if value < 0 || value > int64(^uint(0)>>1) {
		return 0, fmt.Errorf("must be a non-negative integer")
	}
	return int(value), nil
}
