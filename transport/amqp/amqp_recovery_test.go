package amqp

import (
	"context"
	"testing"

	"github.com/prabhatdotdev/weave/core"
)

// TestIsRecoveringMethod verifies that IsRecovering() correctly reflects broker state.
func TestIsRecoveringMethod(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()

	// Initially not recovering
	if broker.IsRecovering() {
		t.Fatalf("IsRecovering() = true, want false initially")
	}

	// Set recovering flag
	broker.closeMu.Lock()
	broker.recovering = true
	broker.closeMu.Unlock()

	if !broker.IsRecovering() {
		t.Fatalf("IsRecovering() = false, want true after setting flag")
	}

	// Clear recovering flag
	broker.closeMu.Lock()
	broker.recovering = false
	broker.closeMu.Unlock()

	if broker.IsRecovering() {
		t.Fatalf("IsRecovering() = true, want false after clearing flag")
	}
}

// TestRecoveryStateTransitions verifies the correct state transitions through recovery.
func TestRecoveryStateTransitions(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()

	// Initial state: not connected, not recovering
	if broker.IsConnected() || broker.IsRecovering() {
		t.Fatalf("Initial: Connected=%v Recovering=%v, want both false", broker.IsConnected(), broker.IsRecovering())
	}

	// Simulate recovery started
	broker.closeMu.Lock()
	broker.recovering = true
	broker.closeMu.Unlock()

	if broker.IsConnected() || !broker.IsRecovering() {
		t.Fatalf("During recovery: Connected=%v Recovering=%v, want false true", broker.IsConnected(), broker.IsRecovering())
	}

	// Simulate recovery success
	broker.closeMu.Lock()
	broker.connected = true
	broker.recovering = false
	broker.closeMu.Unlock()

	if !broker.IsConnected() || broker.IsRecovering() {
		t.Fatalf("After recovery: Connected=%v Recovering=%v, want true false", broker.IsConnected(), broker.IsRecovering())
	}
}

// TestPublishFailsDuringRecovery verifies that Publish fails appropriately during recovery.
func TestPublishFailsDuringRecovery(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()

	// Set recovering state
	broker.closeMu.Lock()
	broker.recovering = true
	broker.closeMu.Unlock()

	// Publish should fail
	msg := core.NewTextMessage("test")
	err = broker.Publish(context.Background(), "test.queue", msg)
	if err == nil {
		t.Fatalf("Publish during recovery returned nil, want error")
	}
}

// TestSubscribeFailsDuringRecovery verifies that Subscribe fails appropriately during recovery.
func TestSubscribeFailsDuringRecovery(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()

	// Set recovering state
	broker.closeMu.Lock()
	broker.recovering = true
	broker.closeMu.Unlock()

	// Subscribe should fail
	err = broker.Subscribe(context.Background(), "test.queue", func(context.Context, *core.Message) error { return nil })
	if err == nil {
		t.Fatalf("Subscribe during recovery returned nil, want error")
	}
}

// TestCallFailsDuringRecovery verifies that Call fails appropriately during recovery.
func TestCallFailsDuringRecovery(t *testing.T) {
	t.Parallel()

	brokerAny, err := NewBroker(core.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}
	broker := brokerAny.(*Broker)
	defer broker.Close()

	// Set recovering state
	broker.closeMu.Lock()
	broker.recovering = true
	broker.closeMu.Unlock()

	// Call should fail
	msg := core.NewTextMessage("test")
	_, err = broker.Call(context.Background(), "test.queue", msg)
	if err == nil {
		t.Fatalf("Call during recovery returned nil, want error")
	}
}
