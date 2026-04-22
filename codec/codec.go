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

// Package codec provides encoding and decoding utilities for message payloads.
package codec

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/prabhatdotdev/weave/core"
	"google.golang.org/protobuf/proto"
)

// Codec defines the interface for message encoding and decoding.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	ContentType() string
}

// jsonCodec implements Codec for JSON encoding.
type jsonCodec struct{}

// JSON is the default JSON codec.
var JSON Codec = &jsonCodec{}

// Protobuf encodes payloads using Protocol Buffers.
var Protobuf Codec = &protobufCodec{}

func (c *jsonCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *jsonCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (c *jsonCodec) ContentType() string {
	return "application/json"
}

// EncodeJSON is a convenience function for JSON encoding.
func EncodeJSON(v any) ([]byte, error) {
	return JSON.Encode(v)
}

// DecodeJSON is a convenience function for JSON decoding.
func DecodeJSON(data []byte, v any) error {
	return JSON.Decode(data, v)
}

// protobufCodec implements Codec for Protocol Buffers payloads.
type protobufCodec struct{}

func (c *protobufCodec) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok || isNilValue(msg) {
		return nil, fmt.Errorf("codec: expected proto.Message, got %T", v)
	}
	return proto.Marshal(msg)
}

func (c *protobufCodec) Decode(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok || isNilValue(msg) {
		return fmt.Errorf("codec: expected proto.Message, got %T", v)
	}
	return proto.Unmarshal(data, msg)
}

func (c *protobufCodec) ContentType() string {
	return "application/x-protobuf"
}

// EncodeProtobuf is a convenience function for protobuf encoding.
func EncodeProtobuf(v proto.Message) ([]byte, error) {
	return Protobuf.Encode(v)
}

// DecodeProtobuf is a convenience function for protobuf decoding.
func DecodeProtobuf(data []byte, v proto.Message) error {
	return Protobuf.Decode(data, v)
}

// MarshalMessage encodes a value into a weave message and sets the content type.
func MarshalMessage(c Codec, v any) (*core.Message, error) {
	if c == nil {
		return nil, errors.New("codec: nil codec")
	}

	body, err := c.Encode(v)
	if err != nil {
		return nil, err
	}

	return core.NewMessage(body).WithContentType(c.ContentType()), nil
}

// UnmarshalMessage decodes a weave message body into the provided value.
func UnmarshalMessage(c Codec, msg *core.Message, v any) error {
	if c == nil {
		return errors.New("codec: nil codec")
	}
	if msg == nil {
		return errors.New("codec: nil message")
	}

	return c.Decode(msg.Body, v)
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
