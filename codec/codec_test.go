package codec

import (
	"errors"
	"strings"
	"testing"

	"github.com/prabhatdotdev/weave/core"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestJSONCodecRoundTrip(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"id":   42,
		"name": "Ada",
	}

	msg, err := MarshalMessage(JSON, payload)
	if err != nil {
		t.Fatalf("MarshalMessage(JSON) error = %v", err)
	}
	if msg.ContentType != JSON.ContentType() {
		t.Fatalf("ContentType = %q, want %q", msg.ContentType, JSON.ContentType())
	}

	var decoded map[string]any
	if err := UnmarshalMessage(JSON, msg, &decoded); err != nil {
		t.Fatalf("UnmarshalMessage(JSON) error = %v", err)
	}
	if decoded["name"] != "Ada" {
		t.Fatalf("decoded name = %v, want %q", decoded["name"], "Ada")
	}
}

func TestProtobufCodecRoundTrip(t *testing.T) {
	t.Parallel()

	payload := wrapperspb.String("user-123")

	msg, err := MarshalMessage(Protobuf, payload)
	if err != nil {
		t.Fatalf("MarshalMessage(Protobuf) error = %v", err)
	}
	if msg.ContentType != Protobuf.ContentType() {
		t.Fatalf("ContentType = %q, want %q", msg.ContentType, Protobuf.ContentType())
	}

	var decoded wrapperspb.StringValue
	if err := UnmarshalMessage(Protobuf, msg, &decoded); err != nil {
		t.Fatalf("UnmarshalMessage(Protobuf) error = %v", err)
	}
	if decoded.Value != payload.Value {
		t.Fatalf("decoded.Value = %q, want %q", decoded.Value, payload.Value)
	}
}

func TestProtobufCodecRejectsNonProtoValues(t *testing.T) {
	t.Parallel()

	_, err := MarshalMessage(Protobuf, map[string]string{"id": "123"})
	if err == nil {
		t.Fatal("MarshalMessage(Protobuf) error = nil, want error")
	}
	if !strings.Contains(err.Error(), "proto.Message") {
		t.Fatalf("error = %q, want proto.Message hint", err)
	}
}

func TestUnmarshalMessageRejectsNilMessage(t *testing.T) {
	t.Parallel()

	var decoded wrapperspb.StringValue
	err := UnmarshalMessage(Protobuf, nil, &decoded)
	if err == nil {
		t.Fatal("UnmarshalMessage(nil) error = nil, want error")
	}
	if err.Error() != "codec: nil message" {
		t.Fatalf("error = %q, want nil message error", err)
	}
}

func TestUnmarshalAndValidateJSON(t *testing.T) {
	t.Parallel()

	type payload struct {
		ID string `json:"id"`
	}

	var decoded payload
	calls := 0
	err := UnmarshalAndValidate(JSON, core.NewMessage([]byte(`{"id":"order-1"}`)), &decoded, func(v *payload) error {
		calls++
		if v.ID == "" {
			return errors.New("missing id")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UnmarshalAndValidate(JSON) error = %v", err)
	}
	if decoded.ID != "order-1" || calls != 1 {
		t.Fatalf("decoded = %#v, validation calls = %d", decoded, calls)
	}
}

func TestUnmarshalAndValidateProtobuf(t *testing.T) {
	t.Parallel()

	msg, err := MarshalMessage(Protobuf, wrapperspb.String("user-123"))
	if err != nil {
		t.Fatalf("MarshalMessage(Protobuf) error = %v", err)
	}

	var decoded wrapperspb.StringValue
	err = UnmarshalAndValidate(Protobuf, msg, &decoded, func(v *wrapperspb.StringValue) error {
		if v.Value == "" {
			return errors.New("missing value")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UnmarshalAndValidate(Protobuf) error = %v", err)
	}
	if decoded.Value != "user-123" {
		t.Fatalf("decoded.Value = %q, want user-123", decoded.Value)
	}
}

func TestUnmarshalAndValidateSkipsValidationAfterDecodeFailure(t *testing.T) {
	t.Parallel()

	var decoded map[string]any
	called := false
	err := UnmarshalAndValidate(JSON, core.NewMessage([]byte(`{`)), &decoded, func(*map[string]any) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("UnmarshalAndValidate(JSON) error = nil, want decode error")
	}
	if called {
		t.Fatal("validator called after decode failure")
	}
}

func TestUnmarshalAndValidateWrapsValidationFailure(t *testing.T) {
	t.Parallel()

	validationErr := errors.New("missing id")
	var decoded map[string]any
	err := UnmarshalAndValidate(JSON, core.NewMessage([]byte(`{}`)), &decoded, func(*map[string]any) error {
		return validationErr
	})
	if !errors.Is(err, validationErr) {
		t.Fatalf("error = %v, want wrapped validation error", err)
	}
	if !strings.Contains(err.Error(), "codec: validation failed") {
		t.Fatalf("error = %q, want validation context", err)
	}
}

func TestUnmarshalAndValidateRejectsNilValidatorBeforeDecode(t *testing.T) {
	t.Parallel()

	decoded := map[string]any{"unchanged": true}
	err := UnmarshalAndValidate(JSON, core.NewMessage([]byte(`{"changed":true}`)), &decoded, nil)
	if err == nil || err.Error() != "codec: nil validator" {
		t.Fatalf("error = %v, want nil validator error", err)
	}
	if len(decoded) != 1 || decoded["unchanged"] != true {
		t.Fatalf("decoded value changed before rejecting nil validator: %#v", decoded)
	}
}
