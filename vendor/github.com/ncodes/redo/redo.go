package redo

import "time"
import "fmt"
import "github.com/cenkalti/backoff"

// Func represents the function to pass to the Redo object
type Func func(stop func()) error

// Default values for ExponentialBackOff.
const (
	DefaultInitialInterval     = 500 * time.Millisecond
	DefaultRandomizationFactor = 0.5
	DefaultMultiplier          = 1.5
	DefaultMaxInterval         = 60 * time.Second
	DefaultMaxElapsedTime      = 15 * time.Minute
)

// BackOffConfig is used to configure the exponential retry implementation
type BackOffConfig struct {
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64
	MaxInterval         time.Duration
	MaxElapsedTime      time.Duration
}

// NewDefaultBackoffConfig returns the default backoff config
func NewDefaultBackoffConfig() *BackOffConfig {
	return &BackOffConfig{
		InitialInterval:     DefaultInitialInterval,
		RandomizationFactor: DefaultRandomizationFactor,
		Multiplier:          DefaultMultiplier,
		MaxInterval:         DefaultMaxInterval,
		MaxElapsedTime:      DefaultMaxElapsedTime,
	}
}

// ErrMaxRetryReached indicates that max retry has been reached
var ErrMaxRetryReached = fmt.Errorf("max retry reached")

// Redo defines a structure that provides the ability
// to run a function continuously as long as the function
// returns an error.
type Redo struct {
	stop    bool
	LastErr error
}

// NewRedo creates a new Redo instance.
func NewRedo() *Redo {
	return &Redo{}
}

// Stop redoing
func (r *Redo) Stop() {
	r.stop = true
}

// Do runs a function. It will continuously retry the function
// if it returns errs abd will only stop if max retries is exceeded
// or explicitly stopped using the stop function passed to the
// running function or the object's Stop function. ErrMaxRetryReached is
// returned if the max retries has reached. Check the LastErr object field
// for the last error returned by the function.
func (r *Redo) Do(maxRetries int, retryDelay time.Duration, f Func) error {
	retryCount := 0
	for !r.stop {

		retryCount++

		if maxRetries > -1 && retryCount > maxRetries {
			return ErrMaxRetryReached
		}

		r.LastErr = f(r.Stop)
		if r.LastErr == nil {
			break
		}

		time.Sleep(retryDelay)
	}
	return r.LastErr
}

// BackOff is similar to Do but will retry the function exponentially.
func (r *Redo) BackOff(backoffConfig *BackOffConfig, f Func) error {

	r.LastErr = nil

	if backoffConfig == nil {
		backoffConfig = NewDefaultBackoffConfig()
	}

	var exb backoff.BackOff = &backoff.ExponentialBackOff{
		InitialInterval:     backoffConfig.InitialInterval,
		RandomizationFactor: backoffConfig.RandomizationFactor,
		Multiplier:          backoffConfig.Multiplier,
		MaxInterval:         backoffConfig.MaxInterval,
		MaxElapsedTime:      backoffConfig.MaxElapsedTime,
		Clock:               backoff.SystemClock,
	}

	err := backoff.Retry(func() error {

		if r.stop {
			return backoff.Permanent(r.LastErr)
		}

		r.LastErr = f(r.Stop)

		if r.stop || r.LastErr == nil {
			return backoff.Permanent(r.LastErr)
		}

		return r.LastErr
	}, exb)

	if r.LastErr == nil && err != nil {
		r.LastErr = err
	}

	return r.LastErr
}
