package main

// Profile Service - Server-only Weave example.
//
// This service demonstrates a "leaf" service pattern - it handles
// incoming requests but doesn't make outbound calls to other services.
//
// Routes:
//   - profiles.get: Get a profile by user ID
//   - profiles.list: List all profiles

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/prabhatdotdev/weave"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
)

// Profile represents a user's profile information.
type Profile struct {
	UserID   string `json:"user_id"`
	Bio      string `json:"bio"`
	Avatar   string `json:"avatar"`
	Location string `json:"location"`
	Website  string `json:"website"`
	Verified bool   `json:"verified"`
}

// GetProfileRequest is the request to get a profile.
type GetProfileRequest struct {
	UserID string `json:"user_id"`
}

// GetProfileResponse is the response containing a profile.
type GetProfileResponse struct {
	Profile *Profile `json:"profile,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// ListProfilesResponse is the response containing all profiles.
type ListProfilesResponse struct {
	Profiles []Profile `json:"profiles"`
}

// In-memory profile store (simulates a database)
var profiles = map[string]Profile{
	"1": {
		UserID:   "1",
		Bio:      "Software engineer and open source enthusiast",
		Avatar:   "https://example.com/avatars/alice.jpg",
		Location: "San Francisco, CA",
		Website:  "https://alice.dev",
		Verified: true,
	},
	"2": {
		UserID:   "2",
		Bio:      "DevOps engineer specializing in Kubernetes",
		Avatar:   "https://example.com/avatars/bob.jpg",
		Location: "Austin, TX",
		Website:  "https://bob.cloud",
		Verified: true,
	},
	"3": {
		UserID:   "3",
		Bio:      "Full-stack developer building cool things",
		Avatar:   "https://example.com/avatars/charlie.jpg",
		Location: "Seattle, WA",
		Website:  "",
		Verified: false,
	},
}

// Global server reference for handlers to send responses
var server *weave.Server

func main() {
	fmt.Println("=== Weave JSON Example - Profile Service ===")
	fmt.Println()

	// Configuration
	config := weave.DefaultConfig()
	config.AMQP.Host = getEnv("RABBITMQ_HOST", "localhost")
	config.AMQP.Port = 5672

	// Create server
	var err error
	server, err = weave.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Register handlers
	server.Handle("profiles.get", handleGetProfile)
	server.Handle("profiles.list", handleListProfiles)

	fmt.Println("Registered routes:")
	fmt.Println("  - profiles.get")
	fmt.Println("  - profiles.list")
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

	// Start the server
	fmt.Println("Profile Service started. Press Ctrl+C to stop.")
	if err := server.Start(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	// Block until context is cancelled
	<-ctx.Done()

	fmt.Println("Profile Service stopped.")
}

// handleGetProfile handles requests to get a profile by user ID.
func handleGetProfile(ctx context.Context, msg *weave.Message) error {
	log.Printf("[profiles.get] Received request: %s", string(msg.Body))

	// Parse request
	var req GetProfileRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return sendResponse(ctx, msg, GetProfileResponse{Error: "invalid request format"})
	}

	// Validate
	if req.UserID == "" {
		return sendResponse(ctx, msg, GetProfileResponse{Error: "user_id is required"})
	}

	// Look up profile
	profile, exists := profiles[req.UserID]
	if !exists {
		log.Printf("[profiles.get] Profile not found for user: %s", req.UserID)
		return sendResponse(ctx, msg, GetProfileResponse{Error: "profile not found"})
	}

	log.Printf("[profiles.get] Found profile for user: %s", req.UserID)
	return sendResponse(ctx, msg, GetProfileResponse{Profile: &profile})
}

// handleListProfiles handles requests to list all profiles.
func handleListProfiles(ctx context.Context, msg *weave.Message) error {
	log.Printf("[profiles.list] Received request")

	// Collect all profiles
	result := make([]Profile, 0, len(profiles))
	for _, profile := range profiles {
		result = append(result, profile)
	}

	log.Printf("[profiles.list] Returning %d profiles", len(result))
	return sendResponse(ctx, msg, ListProfilesResponse{Profiles: result})
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
