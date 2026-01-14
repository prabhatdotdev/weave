package main

// User Service - Server + Client Weave example.
//
// This service demonstrates the combined server/client pattern:
//   - Acts as a SERVER to handle incoming user requests
//   - Acts as a CLIENT to call the Profile Service
//
// Routes:
//   - users.get: Get a user by ID (enriched with profile)
//   - users.list: List all users (enriched with profiles)

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prabhatdotdev/weave"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
)

// User represents a user in our system.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Profile represents profile data from the Profile Service.
type Profile struct {
	UserID   string `json:"user_id"`
	Bio      string `json:"bio"`
	Avatar   string `json:"avatar"`
	Location string `json:"location"`
	Website  string `json:"website"`
	Verified bool   `json:"verified"`
}

// EnrichedUser combines user data with profile data.
type EnrichedUser struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Email   string   `json:"email"`
	Profile *Profile `json:"profile,omitempty"`
}

// Request/Response types
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

type GetProfileRequest struct {
	UserID string `json:"user_id"`
}

type GetProfileResponse struct {
	Profile *Profile `json:"profile,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// In-memory user store (simulates a database)
var users = map[string]User{
	"1": {ID: "1", Name: "Alice Smith", Email: "alice@example.com"},
	"2": {ID: "2", Name: "Bob Johnson", Email: "bob@example.com"},
	"3": {ID: "3", Name: "Charlie Brown", Email: "charlie@example.com"},
}

// Global server and client references
var (
	server        *weave.Server
	profileClient *weave.Client
)

func main() {
	fmt.Println("=== Weave JSON Example - User Service ===")
	fmt.Println()

	// Configuration
	config := weave.DefaultConfig()
	config.AMQP.Host = getEnv("RABBITMQ_HOST", "localhost")
	config.AMQP.Port = 5672

	// Create client for calling Profile Service
	var err error
	profileClient, err = weave.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer profileClient.Close()

	// Create server for handling incoming requests
	server, err = weave.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Register handlers
	server.Handle("users.get", handleGetUser)
	server.Handle("users.list", handleListUsers)

	fmt.Println("Registered routes:")
	fmt.Println("  - users.get")
	fmt.Println("  - users.list")
	fmt.Println()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Connect the client before starting the server
	fmt.Println("Connecting to Profile Service...")
	if err := profileClient.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect client: %v", err)
	}
	fmt.Println("Connected!")
	fmt.Println()

	// Start the server
	fmt.Println("User Service started. Press Ctrl+C to stop.")
	if err := server.Start(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	// Block until context is cancelled
	<-ctx.Done()

	fmt.Println("User Service stopped.")
}

// handleGetUser handles requests to get a user by ID.
func handleGetUser(ctx context.Context, msg *weave.Message) error {
	log.Printf("[users.get] Received request: %s", string(msg.Body))

	// Parse request
	var req GetUserRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return sendResponse(ctx, msg, GetUserResponse{Error: "invalid request format"})
	}

	// Validate
	if req.ID == "" {
		return sendResponse(ctx, msg, GetUserResponse{Error: "id is required"})
	}

	// Look up user
	user, exists := users[req.ID]
	if !exists {
		log.Printf("[users.get] User not found: %s", req.ID)
		return sendResponse(ctx, msg, GetUserResponse{Error: "user not found"})
	}

	// Create enriched user
	enriched := EnrichedUser{
		ID:    user.ID,
		Name:  user.Name,
		Email: user.Email,
	}

	// Fetch profile from Profile Service
	profile, err := getProfile(ctx, user.ID)
	if err != nil {
		log.Printf("[users.get] Warning: could not fetch profile for user %s: %v", user.ID, err)
		// Continue without profile - graceful degradation
	} else {
		enriched.Profile = profile
	}

	log.Printf("[users.get] Returning user: %s", user.ID)
	return sendResponse(ctx, msg, GetUserResponse{User: &enriched})
}

// handleListUsers handles requests to list all users.
func handleListUsers(ctx context.Context, msg *weave.Message) error {
	log.Printf("[users.list] Received request")

	// Collect all users
	result := make([]EnrichedUser, 0, len(users))

	// Fetch profiles concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex
	profilesMap := make(map[string]*Profile)

	for id := range users {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			profile, err := getProfile(ctx, userID)
			if err != nil {
				log.Printf("[users.list] Warning: could not fetch profile for user %s: %v", userID, err)
				return
			}
			mu.Lock()
			profilesMap[userID] = profile
			mu.Unlock()
		}(id)
	}
	wg.Wait()

	// Build enriched users
	for _, user := range users {
		enriched := EnrichedUser{
			ID:      user.ID,
			Name:    user.Name,
			Email:   user.Email,
			Profile: profilesMap[user.ID],
		}
		result = append(result, enriched)
	}

	log.Printf("[users.list] Returning %d users", len(result))
	return sendResponse(ctx, msg, ListUsersResponse{Users: result})
}

// getProfile calls the Profile Service to get a user's profile.
func getProfile(ctx context.Context, userID string) (*Profile, error) {
	// Create request
	reqData, err := json.Marshal(GetProfileRequest{UserID: userID})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Call Profile Service with timeout
	response, err := profileClient.Call(ctx, "profiles.get",
		weave.NewMessage(reqData),
		weave.WithTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("call failed: %w", err)
	}

	// Parse response
	var resp GetProfileResponse
	if err := json.Unmarshal(response.Body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("profile service error: %s", resp.Error)
	}

	return resp.Profile, nil
}

// sendResponse creates a JSON response and sends it back to the caller.
func sendResponse(ctx context.Context, req *weave.Message, data interface{}) error {
	// If no ReplyTo, this is fire-and-forget - nothing to send back
	if req.ReplyTo == "" {
		return nil
	}

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	response := weave.NewMessage(body)
	response.ContentType = "application/json"
	response.CorrelationID = req.CorrelationID

	return server.Publish(ctx, req.ReplyTo, response)
}

// getEnv returns environment variable or default value.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
