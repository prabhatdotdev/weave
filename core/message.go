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
	"bytes"
	"maps"
	"time"
)

// Message represents a message to be sent or received from a message broker.
type Message struct {
	Body          []byte
	CorrelationID string
	ReplyTo       string
	Headers       map[string]string
	ContentType   string
	MessageID     string
	Timestamp     time.Time
	Subject       string
	Partition     int32
	Offset        int64
}

// NewMessage creates a new Message with the given body.
func NewMessage(body []byte) *Message {
	return &Message{
		Body:      body,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}
}

// NewTextMessage creates a new Message with a string body.
func NewTextMessage(body string) *Message {
	return NewMessage([]byte(body))
}

// WithCorrelationID sets the correlation ID and returns the message for chaining.
func (m *Message) WithCorrelationID(id string) *Message {
	m.CorrelationID = id
	return m
}

// WithReplyTo sets the reply destination and returns the message for chaining.
func (m *Message) WithReplyTo(replyTo string) *Message {
	m.ReplyTo = replyTo
	return m
}

// WithHeader adds a header and returns the message for chaining.
func (m *Message) WithHeader(key, value string) *Message {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
	return m
}

// WithContentType sets the content type and returns the message for chaining.
func (m *Message) WithContentType(contentType string) *Message {
	m.ContentType = contentType
	return m
}

// WithSubject sets the subject/routing key and returns the message for chaining.
func (m *Message) WithSubject(subject string) *Message {
	m.Subject = subject
	return m
}

// Clone creates a deep copy of the message.
func (m *Message) Clone() *Message {
	clone := &Message{
		Body:          bytes.Clone(m.Body),
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		ContentType:   m.ContentType,
		MessageID:     m.MessageID,
		Timestamp:     m.Timestamp,
		Subject:       m.Subject,
		Partition:     m.Partition,
		Offset:        m.Offset,
	}
	clone.Headers = maps.Clone(m.Headers)
	return clone
}

// BodyString returns the body as a string.
func (m *Message) BodyString() string {
	return string(m.Body)
}

// GetHeader returns a header value, or empty string if not found.
func (m *Message) GetHeader(key string) string {
	if m.Headers == nil {
		return ""
	}
	return m.Headers[key]
}
