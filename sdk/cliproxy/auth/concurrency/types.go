package concurrency

import "errors"

var (
	ErrInvalidLimits      = errors.New("invalid account concurrency limits")
	ErrAccountQueueFull   = errors.New("account concurrency queue is full")
	ErrGlobalQueueFull    = errors.New("global account concurrency queue is full")
	ErrAccountUnavailable = errors.New("account concurrency account is unavailable")
)

// Limits describe admission capacity for one account.
type Limits struct {
	MaxActive  int
	MaxWaiting int
}

// Lease owns one active account slot until Release is called.
type Lease interface {
	Release()
}

// Snapshot is a point-in-time view of one account's coordinator state.
type Snapshot struct {
	Active        int
	Waiting       int
	Limit         int
	GlobalWaiting int
}
