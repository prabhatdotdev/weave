package core

import (
	"encoding/json"
	"maps"
	"time"
)

const (
	// ErrorContentType is the standard content type for structured error payloads.
	ErrorContentType = "application/vnd.weave.error+json"
	// DeadLetterContentType is the standard content type for dead-letter envelopes.
	DeadLetterContentType = "application/vnd.weave.dead-letter+json"
)

// ErrorPayload is a standardized structured error body for RPC and dead-letter
// usage.
type ErrorPayload struct {
	Code          string         `json:"code,omitempty"`
	Message       string         `json:"message"`
	Retryable     bool           `json:"retryable,omitempty"`
	CorrelationID string         `json:"correlation_id,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
	Timestamp     time.Time      `json:"timestamp"`
}

// NewErrorPayload constructs a structured error payload with a stable timestamp.
func NewErrorPayload(code string, err error) ErrorPayload {
	payload := ErrorPayload{
		Code:      code,
		Timestamp: time.Now().UTC(),
	}
	if err != nil {
		payload.Message = err.Error()
	}
	return payload
}

// EncodeErrorPayload serializes a structured error payload.
func EncodeErrorPayload(payload ErrorPayload) ([]byte, error) {
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now().UTC()
	}
	return json.Marshal(payload)
}

// DecodeErrorPayload deserializes a structured error payload.
func DecodeErrorPayload(data []byte) (ErrorPayload, error) {
	var payload ErrorPayload
	err := json.Unmarshal(data, &payload)
	return payload, err
}

// NewErrorMessage creates a message with the standardized structured error body.
func NewErrorMessage(payload ErrorPayload) (*Message, error) {
	body, err := EncodeErrorPayload(payload)
	if err != nil {
		return nil, err
	}

	msg := NewMessage(body).WithContentType(ErrorContentType)
	msg.CorrelationID = payload.CorrelationID
	return msg, nil
}

// DecodeErrorMessage decodes a standardized structured error message.
func DecodeErrorMessage(msg *Message) (ErrorPayload, error) {
	if msg == nil {
		return ErrorPayload{}, nil
	}
	payload, err := DecodeErrorPayload(msg.Body)
	if payload.CorrelationID == "" {
		payload.CorrelationID = msg.CorrelationID
	}
	return payload, err
}

// DeadLetterMessage captures the original message metadata for dead-lettering.
type DeadLetterMessage struct {
	Body          []byte            `json:"body"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	ReplyTo       string            `json:"reply_to,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	ContentType   string            `json:"content_type,omitempty"`
	MessageID     string            `json:"message_id,omitempty"`
	Timestamp     time.Time         `json:"timestamp,omitempty"`
	Subject       string            `json:"subject,omitempty"`
	Partition     int32             `json:"partition,omitempty"`
	Offset        int64             `json:"offset,omitempty"`
}

// DeadLetterOptions annotates dead-letter envelopes with failure context.
type DeadLetterOptions struct {
	Backend     string
	Destination string
	Handler     string
	Attempt     int
	Code        string
	Retryable   bool
	Details     map[string]any
}

// DeadLetterEnvelope is a transport-agnostic dead-letter payload format.
type DeadLetterEnvelope struct {
	FailedAt    time.Time         `json:"failed_at"`
	Backend     string            `json:"backend,omitempty"`
	Destination string            `json:"destination,omitempty"`
	Handler     string            `json:"handler,omitempty"`
	Attempt     int               `json:"attempt,omitempty"`
	Error       ErrorPayload      `json:"error"`
	Original    DeadLetterMessage `json:"original"`
}

// NewDeadLetterEnvelope builds a dead-letter envelope for a failed message.
func NewDeadLetterEnvelope(msg *Message, err error, opts DeadLetterOptions) DeadLetterEnvelope {
	envelope := DeadLetterEnvelope{
		FailedAt:    time.Now().UTC(),
		Backend:     opts.Backend,
		Destination: opts.Destination,
		Handler:     opts.Handler,
		Attempt:     opts.Attempt,
		Error: ErrorPayload{
			Code:          opts.Code,
			Message:       errorMessage(err),
			Retryable:     opts.Retryable,
			CorrelationID: correlationIDFromMessage(msg),
			Details:       maps.Clone(opts.Details),
			Timestamp:     time.Now().UTC(),
		},
	}
	if msg != nil {
		envelope.Original = DeadLetterMessage{
			Body:          append([]byte(nil), msg.Body...),
			CorrelationID: msg.CorrelationID,
			ReplyTo:       msg.ReplyTo,
			Headers:       maps.Clone(msg.Headers),
			ContentType:   msg.ContentType,
			MessageID:     msg.MessageID,
			Timestamp:     msg.Timestamp,
			Subject:       msg.Subject,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
		}
	}
	return envelope
}

// Message encodes the dead-letter envelope as a Weave message.
func (d DeadLetterEnvelope) Message() (*Message, error) {
	if d.FailedAt.IsZero() {
		d.FailedAt = time.Now().UTC()
	}
	if d.Error.Timestamp.IsZero() {
		d.Error.Timestamp = d.FailedAt
	}

	body, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	msg := NewMessage(body).WithContentType(DeadLetterContentType)
	msg.CorrelationID = d.Error.CorrelationID
	return msg, nil
}

// DecodeDeadLetterMessage decodes a dead-letter envelope from a message body.
func DecodeDeadLetterMessage(msg *Message) (DeadLetterEnvelope, error) {
	if msg == nil {
		return DeadLetterEnvelope{}, nil
	}
	var envelope DeadLetterEnvelope
	err := json.Unmarshal(msg.Body, &envelope)
	if envelope.Error.CorrelationID == "" {
		envelope.Error.CorrelationID = msg.CorrelationID
	}
	return envelope, err
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func correlationIDFromMessage(msg *Message) string {
	if msg == nil {
		return ""
	}
	return msg.CorrelationID
}
