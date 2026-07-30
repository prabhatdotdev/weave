// Copyright 2025 PrabhatDotDev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"errors"
	"fmt"
)

// ErrNotConnected is returned when an operation is attempted on a disconnected broker.
type ErrNotConnected struct {
	Backend string
}

func (e *ErrNotConnected) Error() string {
	return fmt.Sprintf("%s: not connected to broker", e.Backend)
}

// ErrConnectionLost is returned when the connection to the broker is lost.
type ErrConnectionLost struct {
	Backend string
	Cause   error
}

func (e *ErrConnectionLost) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: connection lost: %v", e.Backend, e.Cause)
	}
	return fmt.Sprintf("%s: connection lost", e.Backend)
}

func (e *ErrConnectionLost) Unwrap() error {
	return e.Cause
}

// ErrTimeout is returned when an operation times out.
type ErrTimeout struct {
	Operation string
	Duration  string
}

func (e *ErrTimeout) Error() string {
	return fmt.Sprintf("operation %s timed out after %s", e.Operation, e.Duration)
}

// ErrCircuitOpen is returned when RPC retries are blocked by an open circuit breaker.
type ErrCircuitOpen struct {
	Operation  string
	RetryAfter string
}

func (e *ErrCircuitOpen) Error() string {
	if e.RetryAfter != "" {
		return fmt.Sprintf("operation %s blocked by open circuit breaker; retry after %s", e.Operation, e.RetryAfter)
	}
	return fmt.Sprintf("operation %s blocked by open circuit breaker", e.Operation)
}

// ErrPublishFailed is returned when message publishing fails.
type ErrPublishFailed struct {
	Backend     string
	Destination string
	Cause       error
}

func (e *ErrPublishFailed) Error() string {
	return fmt.Sprintf("%s: failed to publish to %s: %v", e.Backend, e.Destination, e.Cause)
}

func (e *ErrPublishFailed) Unwrap() error {
	return e.Cause
}

// ErrSubscribeFailed is returned when subscription fails.
type ErrSubscribeFailed struct {
	Backend     string
	Destination string
	Cause       error
}

func (e *ErrSubscribeFailed) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: failed to subscribe to %s: %v", e.Backend, e.Destination, e.Cause)
	}
	return fmt.Sprintf("%s: failed to subscribe to %s", e.Backend, e.Destination)
}

func (e *ErrSubscribeFailed) Unwrap() error {
	return e.Cause
}

// ErrUnsupportedOperation is returned when a backend doesn't support an operation.
type ErrUnsupportedOperation struct {
	Backend   string
	Operation string
}

func (e *ErrUnsupportedOperation) Error() string {
	return fmt.Sprintf("%s: operation %s is not supported", e.Backend, e.Operation)
}

// ErrConnectionFailed is returned when initial connection fails.
type ErrConnectionFailed struct {
	Backend string
	Address string
	Cause   error
}

func (e *ErrConnectionFailed) Error() string {
	return fmt.Sprintf("%s: failed to connect to %s: %v", e.Backend, e.Address, e.Cause)
}

func (e *ErrConnectionFailed) Unwrap() error {
	return e.Cause
}

// Sentinel errors for common conditions.
var (
	ErrClosed           = errors.New("broker is closed")
	ErrNoReplyTo        = errors.New("no reply-to destination specified")
	ErrAlreadyConnected = errors.New("broker is already connected")
	ErrInvalidConfig    = errors.New("invalid configuration")
)

// IsNotConnected returns true if the error indicates the broker is not connected.
func IsNotConnected(err error) bool {
	var e *ErrNotConnected
	return errors.As(err, &e)
}

// IsConnectionLost returns true if the error indicates the connection was lost.
func IsConnectionLost(err error) bool {
	var e *ErrConnectionLost
	return errors.As(err, &e)
}

// IsTimeout returns true if the error indicates a timeout.
func IsTimeout(err error) bool {
	var e *ErrTimeout
	return errors.As(err, &e)
}

// IsCircuitOpen returns true if the error indicates an open circuit breaker.
func IsCircuitOpen(err error) bool {
	var e *ErrCircuitOpen
	return errors.As(err, &e)
}

// IsUnsupportedOperation returns true if the error indicates an unsupported operation.
func IsUnsupportedOperation(err error) bool {
	var e *ErrUnsupportedOperation
	return errors.As(err, &e)
}
