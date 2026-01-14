package main

// API Client - Demonstrates client-only usage of Weave.
//
// This client simulates an external application (like a web API)
// that calls the User Service to fetch data.
//
// Demonstrates:
//   - Creating a Weave client
//   - Making RPC calls with Call()
//   - Fire-and-forget publishing with Publish()
//   - Error handling and timeouts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prabhatdotdev/weave"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
)

// Data types matching the services
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Profile struct {
	UserID   string `json:"user_id"`
	Bio      string `json:"bio"`
	Avatar   string `json:"avatar"`
	Location string `json:"location"`
	Website  string `json:"website"`
	Verified bool   `json:"verified"`
}

type EnrichedUser struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Email   string   `json:"email"`
	Profile *Profile `json:"profile,omitempty"`
}

type GetUserRequest struct {
	ID string `json:"id"`
}

type GetUserResponse struct {
	User  *EnrichedUser `json:"user,omitempty"`
	Error string        `json:"error,omitempty"`
}

type ListUsersResponse struct {
	Users []EnrichedUser `json:"users"`
}

type UserEvent struct {
	EventType string    `json:"event_type"`
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	fmt.Println("=== Weave JSON Example - API Client ===")
	fmt.Println()

	// Configuration
	config := weave.DefaultConfig()
	config.AMQP.Host = getEnv("RABBITMQ_HOST", "localhost")
	config.AMQP.Port = 5672

	// Create client
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

	// Demo 1: Get a single user by ID
	fmt.Println("--- Demo 1: Get User by ID ---")
	if err := demoGetUser(ctx, client, "1"); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	// Demo 2: Get a non-existent user
	fmt.Println("--- Demo 2: Get Non-Existent User ---")
	if err := demoGetUser(ctx, client, "999"); err != nil {
		log.Printf("Expected error: %v", err)
	}
	fmt.Println()

	// Demo 3: List all users
	fmt.Println("--- Demo 3: List All Users ---")
	if err := demoListUsers(ctx, client); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	// Demo 4: Fire-and-forget event publishing
	fmt.Println("--- Demo 4: Publish Event (Fire-and-Forget) ---")
	if err := demoPublishEvent(ctx, client, "1"); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	// Demo 5: Call with timeout
	fmt.Println("--- Demo 5: Call with Short Timeout ---")
	if err := demoTimeoutCall(ctx, client); err != nil {
		log.Printf("Timeout example: %v", err)
	}

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}

// demoGetUser demonstrates getting a user by ID via RPC call.
func demoGetUser(ctx context.Context, client *weave.Client, userID string) error {
	fmt.Printf("Fetching user with ID: %s\n", userID)

	// Create request
	reqData, err := json.Marshal(GetUserRequest{ID: userID})
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Make RPC call
	startTime := time.Now()
	response, err := client.Call(ctx, "users.get",
		weave.NewMessage(reqData),
		weave.WithTimeout(10*time.Second))
	duration := time.Since(startTime)
	if err != nil {
		return fmt.Errorf("call failed: %w", err)
	}

	// Parse response
	var resp GetUserResponse
	if err := json.Unmarshal(response.Body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("service error: %s", resp.Error)
	}

	// Display result
	fmt.Printf("Response received in %v\n", duration)
	fmt.Printf("User: %s (%s)\n", resp.User.Name, resp.User.Email)
	if resp.User.Profile != nil {
		fmt.Printf("Profile: %s - %s\n", resp.User.Profile.Bio, resp.User.Profile.Location)
		fmt.Printf("Verified: %v\n", resp.User.Profile.Verified)
	}

	return nil
}

// demoListUsers demonstrates listing all users.
func demoListUsers(ctx context.Context, client *weave.Client) error {
	fmt.Println("Fetching all users...")

	// Make RPC call
	startTime := time.Now()
	response, err := client.Call(ctx, "users.list",
		weave.NewMessage([]byte("{}")),
		weave.WithTimeout(15*time.Second))
	duration := time.Since(startTime)
	if err != nil {
		return fmt.Errorf("call failed: %w", err)
	}

	// Parse response
	var resp ListUsersResponse
	if err := json.Unmarshal(response.Body, &resp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Display results
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

// demoPublishEvent demonstrates fire-and-forget publishing.
func demoPublishEvent(ctx context.Context, client *weave.Client, userID string) error {
	event := UserEvent{
		EventType: "user.viewed",
		UserID:    userID,
		Timestamp: time.Now(),
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to create event: %w", err)
	}

	// Fire-and-forget - no response expected
	msg := weave.NewMessage(eventData)
	msg.ContentType = "application/json"
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

// demoTimeoutCall demonstrates timeout handling.
func demoTimeoutCall(ctx context.Context, client *weave.Client) error {
	fmt.Println("Making call with very short timeout (expecting timeout)...")

	reqData, _ := json.Marshal(GetUserRequest{ID: "1"})

	// Very short timeout to demonstrate timeout handling
	_, err := client.Call(ctx, "users.get",
		weave.NewMessage(reqData),
		weave.WithTimeout(1*time.Millisecond)) // Extremely short timeout

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

// getEnv returns environment variable or default value.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
