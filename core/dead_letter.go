package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// DeadLetterHandler retries handler, then publishes a NewDeadLetterEnvelope on
// terminal failure. It returns nil only after processing or publishing succeeds.
// Use manual acknowledgments: WithAutoAck cannot provide this guarantee.
//
// metadata describes the original delivery; destination is the dead-letter
// target. Attempt is set to the actual number of handler calls. Details are
// snapshotted as JSON at construction. Publishes default to persistent delivery
// and always use mandatory AMQP routing. Provision the target before use.
//
// Failed or interrupted handoffs return ErrDeadLetterFailed, which AMQP and Kafka
// preserve for redelivery regardless of the handler error policy. Handoffs are
// not atomic with source acknowledgments; redelivery can produce duplicates.
func DeadLetterHandler(handler Handler, policy RetryPolicy, publisher Publisher, destination string, metadata DeadLetterOptions, publishOpts ...PublishOption) (Handler, error) {
	if handler == nil || publisher == nil || strings.TrimSpace(destination) == "" {
		return nil, fmt.Errorf("%w: dead-letter handler, publisher, and destination are required", ErrInvalidConfig)
	}
	value := reflect.ValueOf(publisher)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, fmt.Errorf("%w: dead-letter publisher is nil", ErrInvalidConfig)
		}
	}
	options := []PublishOption{WithPersistent()}
	for _, option := range publishOpts {
		if option == nil {
			return nil, fmt.Errorf("%w: dead-letter publish option is nil", ErrInvalidConfig)
		}
		options = append(options, option)
	}
	options = append(options, WithMandatory())
	details := make(map[string]any, len(metadata.Details))
	for key, detail := range metadata.Details {
		encoded, err := json.Marshal(detail)
		if err != nil {
			return nil, fmt.Errorf("%w: dead-letter detail %q: %w", ErrInvalidConfig, key, err)
		}
		details[key] = json.RawMessage(encoded)
	}
	metadata.Details = details
	retryPolicy := policy
	retryPolicy.Retryable = func(err error) bool {
		return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) &&
			(policy.Retryable == nil || policy.Retryable(err))
	}

	return func(ctx context.Context, msg *Message) (result error) {
		// A panic before a confirmed handoff must also preserve the source delivery.
		defer func() {
			if recovered := recover(); recovered != nil {
				result = &ErrDeadLetterFailed{Destination: destination, Cause: fmt.Errorf("dead-letter handler panic: %v", recovered)}
			}
		}()
		failed := func(cause error) error {
			return &ErrDeadLetterFailed{Destination: destination, Cause: cause}
		}
		if ctx == nil || msg == nil {
			return failed(fmt.Errorf("%w: dead-letter context and message are required", ErrInvalidConfig))
		}
		original := msg.Clone()
		attempts := 0
		err := RetryHandler(func(attemptCtx context.Context, msg *Message) error {
			attempts = RetryAttempt(attemptCtx)
			return handler(attemptCtx, msg)
		}, retryPolicy)(ctx, msg)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return failed(errors.Join(err, ctx.Err()))
		}
		annotations := metadata
		annotations.Attempt = attempts
		deadLetter, encodeErr := NewDeadLetterEnvelope(original, err, annotations).Message()
		if encodeErr != nil {
			return failed(errors.Join(err, encodeErr))
		}
		if publishErr := publisher.Publish(ctx, destination, deadLetter, options...); publishErr != nil {
			return failed(errors.Join(err, publishErr))
		}
		return nil
	}, nil
}
