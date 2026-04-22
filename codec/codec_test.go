package codec

import (
	"strings"
	"testing"

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
