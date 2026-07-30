package weave_test

import (
	"errors"
	"testing"

	"github.com/prabhatdotdev/weave"
)

func TestUnmarshalAndValidate(t *testing.T) {
	type payload struct {
		ID string `json:"id"`
	}

	var decoded payload
	err := weave.UnmarshalAndValidate(weave.JSON, weave.NewMessage([]byte(`{"id":"order-1"}`)), &decoded, func(v *payload) error {
		if v.ID == "" {
			return errors.New("missing id")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UnmarshalAndValidate() error = %v", err)
	}
	if decoded.ID != "order-1" {
		t.Fatalf("decoded.ID = %q, want order-1", decoded.ID)
	}
}
