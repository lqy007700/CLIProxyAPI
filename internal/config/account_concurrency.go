package config

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultAccountConcurrencyMaxTotalWait    = "30s"
	defaultAccountConcurrencyMaxSwitches     = 2
	defaultAccountConcurrencyMaxTotalWaiters = 10_000
	defaultAccountConcurrencyStore           = "memory"
	maxAccountConcurrencyMaxSwitches         = 100
	maxAccountConcurrencyMaxTotalWaiters     = 100_000
)

// AccountConcurrencyConfig controls local per-account admission for OAuth credentials.
type AccountConcurrencyConfig struct {
	Enabled            bool   `yaml:"enabled,omitempty" json:"enabled"`
	MaxTotalWait       string `yaml:"max-total-wait,omitempty" json:"max-total-wait"`
	MaxAccountSwitches int    `yaml:"max-account-switches,omitempty" json:"max-account-switches"`
	MaxTotalWaiters    int    `yaml:"max-total-waiters,omitempty" json:"max-total-waiters"`
	Store              string `yaml:"store,omitempty" json:"store"`

	maxTotalWaitPresent       bool
	maxAccountSwitchesPresent bool
	maxTotalWaitersPresent    bool
	storePresent              bool
}

// DefaultAccountConcurrencyConfig returns the disabled, backward-compatible defaults.
func DefaultAccountConcurrencyConfig() AccountConcurrencyConfig {
	return AccountConcurrencyConfig{
		MaxTotalWait:       defaultAccountConcurrencyMaxTotalWait,
		MaxAccountSwitches: defaultAccountConcurrencyMaxSwitches,
		MaxTotalWaiters:    defaultAccountConcurrencyMaxTotalWaiters,
		Store:              defaultAccountConcurrencyStore,
	}
}

// UnmarshalYAML preserves explicit zero values for queue and account-switch limits.
func (c *AccountConcurrencyConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawAccountConcurrencyConfig struct {
		Enabled            bool   `yaml:"enabled"`
		MaxTotalWait       string `yaml:"max-total-wait"`
		MaxAccountSwitches int    `yaml:"max-account-switches"`
		MaxTotalWaiters    int    `yaml:"max-total-waiters"`
		Store              string `yaml:"store"`
	}
	var raw rawAccountConcurrencyConfig
	if errDecode := value.Decode(&raw); errDecode != nil {
		return errDecode
	}
	*c = AccountConcurrencyConfig{
		Enabled:                   raw.Enabled,
		MaxTotalWait:              raw.MaxTotalWait,
		MaxAccountSwitches:        raw.MaxAccountSwitches,
		MaxTotalWaiters:           raw.MaxTotalWaiters,
		Store:                     raw.Store,
		maxTotalWaitPresent:       accountConcurrencyFieldPresent(value, "max-total-wait"),
		maxAccountSwitchesPresent: accountConcurrencyFieldPresent(value, "max-account-switches"),
		maxTotalWaitersPresent:    accountConcurrencyFieldPresent(value, "max-total-waiters"),
		storePresent:              accountConcurrencyFieldPresent(value, "store"),
	}
	return nil
}

func accountConcurrencyFieldPresent(value *yaml.Node, field string) bool {
	if value == nil || value.Kind != yaml.MappingNode {
		return false
	}
	for index := 0; index+1 < len(value.Content); index += 2 {
		if value.Content[index].Value == field {
			return true
		}
	}
	return false
}

// WithDefaults fills only fields omitted by the operator.
func (c AccountConcurrencyConfig) WithDefaults() AccountConcurrencyConfig {
	defaults := DefaultAccountConcurrencyConfig()
	if !c.maxTotalWaitPresent && strings.TrimSpace(c.MaxTotalWait) == "" {
		c.MaxTotalWait = defaults.MaxTotalWait
	}
	if !c.maxAccountSwitchesPresent && c.MaxAccountSwitches == 0 {
		c.MaxAccountSwitches = defaults.MaxAccountSwitches
	}
	if !c.maxTotalWaitersPresent && c.MaxTotalWaiters == 0 {
		c.MaxTotalWaiters = defaults.MaxTotalWaiters
	}
	if !c.storePresent && strings.TrimSpace(c.Store) == "" {
		c.Store = defaults.Store
	}
	return c
}

// MaxTotalWaitDuration returns the parsed request wait budget.
func (c AccountConcurrencyConfig) MaxTotalWaitDuration() time.Duration {
	duration, _ := time.ParseDuration(strings.TrimSpace(c.MaxTotalWait))
	return duration
}

// ValidateAccountConcurrency validates the local coordinator settings.
func ValidateAccountConcurrency(c AccountConcurrencyConfig) error {
	duration, errDuration := time.ParseDuration(strings.TrimSpace(c.MaxTotalWait))
	if errDuration != nil {
		return fmt.Errorf("account concurrency max total wait: %w", errDuration)
	}
	if duration < 100*time.Millisecond || duration > 5*time.Minute {
		return fmt.Errorf("account concurrency max total wait must be between 100ms and 5m")
	}
	if c.MaxAccountSwitches < 0 || c.MaxAccountSwitches > maxAccountConcurrencyMaxSwitches {
		return fmt.Errorf("account concurrency max account switches must be between 0 and %d", maxAccountConcurrencyMaxSwitches)
	}
	if c.MaxTotalWaiters < 1 || c.MaxTotalWaiters > maxAccountConcurrencyMaxTotalWaiters {
		return fmt.Errorf("account concurrency max total waiters must be between 1 and %d", maxAccountConcurrencyMaxTotalWaiters)
	}
	if !strings.EqualFold(strings.TrimSpace(c.Store), defaultAccountConcurrencyStore) {
		return fmt.Errorf("account concurrency store must be memory")
	}
	return nil
}

// NormalizeSessionAffinityCapacityPolicy applies and validates the routing capacity policy.
func NormalizeSessionAffinityCapacityPolicy(cfg *Config) error {
	if cfg == nil {
		return nil
	}
	policy := strings.ToLower(strings.TrimSpace(cfg.Routing.SessionAffinityCapacityPolicy))
	if policy == "" {
		policy = "strict-wait"
	}
	switch policy {
	case "strict-wait", "wait-then-switch":
		cfg.Routing.SessionAffinityCapacityPolicy = policy
		return nil
	default:
		return fmt.Errorf("routing session affinity capacity policy must be strict-wait or wait-then-switch")
	}
}
