package connection

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/prabhatdotdev/weave/core"
)

type testClient struct{ closed chan struct{} }

func (c *testClient) Close() error { close(c.closed); return nil }

func TestOpenClosesClientCreatedAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	client := &testClient{closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := Open(ctx, nil, net.Dialer{}, func(*Dialer) (*testClient, error) {
			close(entered)
			<-release
			return client, nil
		})
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Open() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Open() did not return on cancellation")
	}
	close(release)
	select {
	case <-client.closed:
	case <-time.After(time.Second):
		t.Fatal("client created after cancellation was leaked")
	}
}

func TestDialerInterruptsHandshakeOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, accepted := listenForConnection(t)
	stopped := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		_, err := Open(ctx, nil, net.Dialer{}, func(dialer *Dialer) (net.Conn, error) {
			client, err := dialer.Dial("tcp", listener.Addr().String())
			if err == nil {
				_, err = client.Read(make([]byte, 1))
			}
			stopped <- err
			return nil, err
		})
		done <- err
	}()
	select {
	case server := <-accepted:
		defer server.Close()
	case <-time.After(time.Second):
		t.Fatal("connection did not reach the handshake")
	}
	cancel()
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatal("canceled handshake read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("handshake socket remained open after cancellation")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v", err)
	}
}

func TestSuccessfulOpenOutlivesConnectContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener, accepted := listenForConnection(t)
	var dialer *Dialer
	client, err := Open(ctx, nil, net.Dialer{}, func(d *Dialer) (net.Conn, error) {
		dialer = d
		return d.Dial("tcp", listener.Addr().String())
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	defer server.Close()
	cancel()
	dialer.abort()
	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("successful connection was closed with its setup context: %v", err)
	}
	written := make(chan error, 1)
	go func() { _, err := server.Write([]byte("x")); written <- err }()
	if _, err := client.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func listenForConnection(t *testing.T) (net.Listener, <-chan net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	return listener, accepted
}

func TestRetryWaitStopsForCancellationDeadlineAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Wait(ctx, nil, nil, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry wait returned %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := Wait(ctx, nil, nil, time.Hour); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired retry wait returned %v", err)
	}
	closed := make(chan struct{})
	close(closed)
	if err := Wait(context.Background(), closed, nil, time.Hour); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("closed broker retry wait returned %v", err)
	}
	if err := Wait(context.Background(), nil, nil, time.Millisecond); err != nil {
		t.Fatalf("elapsed retry wait returned %v", err)
	}
}
