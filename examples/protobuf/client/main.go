package main

// API Client - Demonstrates client-only usage of Weave with Protocol Buffers.
//
// This client simulates an external application (like a web API)
// that calls the User Service to fetch data using protobuf serialization.
//
// Demonstrates:
//   - Creating a Weave client
//   - Making RPC calls with Call() using protobuf
//   - Fire-and-forget publishing with Publish()
//   - Error handling and timeouts

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prabhatdotdev/weave"
	pb "github.com/prabhatdotdev/weave/examples/protobuf/proto"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
	"google.golang.org/protobuf/proto"
)

func main() {
	fmt.Println("=== Weave Protobuf Example - API Client ===")
	fmt.Println()

	config := weave.DefaultConfig()
	config.AMQP.Host = getEnv("RABBITMQ_HOST", "localhost")
	config.AMQP.Port = 5672

	client, err := weave.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	fmt.Println("Connecting to message broker...")
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	fmt.Println("Connected!")
	fmt.Println()

	fmt.Println("--- Demo 1: Get User by ID ---")
	if err := demoGetUser(ctx, client, "1"); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	fmt.Println("--- Demo 2: Get Non-Existent User ---")
	if err := demoGetUser(ctx, client, "999"); err != nil {
		log.Printf("Expected error: %v", err)
	}
	fmt.Println()

	fmt.Println("--- Demo 3: List All Users ---")
	if err := demoListUsers(ctx, client); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	fmt.Println("--- Demo 4: Publish Event (Fire-and-Forget) ---")
	if err := demoPublishEvent(ctx, client, "1"); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	fmt.Println("--- Demo 5: Call with Short Timeout ---")
	if err := demoTimeoutCall(ctx, client); err != nil {
		log.Printf("Timeout example: %v", err)
	}

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}

func demoGetUser(ctx context.Context, client *weave.Client, userID string) error {
	fmt.Printf("Fetching user with ID: %s\n", userID)

	reqData, err := proto.Marshal(&pb.GetUserRequest{Id: userID})
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	startTime := time.Now()
	response, err := client.Call(ctx, "users.get",
		weave.NewMessage(reqData),
		weave.WithTimeout(10*time.Second))
	duration := time.Since(startTime)
	if err != nil {
		return fmt.Errorf("call failed: %w", err)
	}

	var resp pb.GetUserResponse
	if err := proto.Unmarshal(response.Body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("service error: %s", resp.Error)
	}

	fmt.Printf("Response received in %v\n", duration)
	fmt.Printf("User: %s (%s)\n", resp.User.Name, resp.User.Email)
	if resp.User.Profile != nil {
		fmt.Printf("Profile: %s - %s\n", resp.User.Profile.Bio, resp.User.Profile.Location)
		fmt.Printf("Verified: %v\n", resp.User.Profile.Verified)
	}

	return nil
}

func demoListUsers(ctx context.Context, client *weave.Client) error {
	fmt.Println("Fetching all users...")

	reqData, err := proto.Marshal(&pb.ListUsersRequest{})
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	startTime := time.Now()
	response, err := client.Call(ctx, "users.list",
		weave.NewMessage(reqData),
		weave.WithTimeout(15*time.Second))
	duration := time.Since(startTime)
	if err != nil {
		return fmt.Errorf("call failed: %w", err)
	}

	var resp pb.ListUsersResponse
	if err := proto.Unmarshal(response.Body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	fmt.Printf("Response received in %v\n", duration)
	fmt.Printf("Found %d users:\n", len(resp.Users))
	for i, user := range resp.Users {
		verified := "✗"
		if user.Profile != nil && user.Profile.Verified {
			verified = "✓"
		}
		fmt.Printf("  %d. %s (%s) [%s]\n", i+1, user.Name, user.Email, verified)
	}

	return nil
}

func demoPublishEvent(ctx context.Context, client *weave.Client, userID string) error {
	event := &pb.UserEvent{
		EventType: "user.viewed",
		UserId:    userID,
		Timestamp: time.Now().Unix(),
	}

	eventData, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to create event: %w", err)
	}

	msg := weave.NewMessage(eventData)
	msg.ContentType = "application/x-protobuf"
	msg.Headers = map[string]string{
		"X-Event-Type": event.EventType,
		"X-Source":     "api-client",
	}

	if err := client.Publish(ctx, "events.user", msg); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	fmt.Printf("Published event: %s for user %s\n", event.EventType, userID)
	return nil
}

func demoTimeoutCall(ctx context.Context, client *weave.Client) error {
	fmt.Println("Making call with very short timeout (expecting timeout)...")

	reqData, _ := proto.Marshal(&pb.GetUserRequest{Id: "1"})

	_, err := client.Call(ctx, "users.get",
		weave.NewMessage(reqData),
		weave.WithTimeout(1*time.Millisecond))

	if err != nil {
		if weave.IsTimeout(err) {
			fmt.Println("Request timed out as expected")
			return nil
		}
		return err
	}

	fmt.Println("Call succeeded (service was very fast!)")
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
