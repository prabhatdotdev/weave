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

import "encoding/json"

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
