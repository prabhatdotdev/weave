package main

// Profile Service - Server-only Weave example with Protocol Buffers.
//
// This service demonstrates a "leaf" service pattern using protobuf serialization.
// It handles incoming requests but doesn't make outbound calls to other services.
//
// Routes:
//   - profiles.get: Get a profile by user ID
//   - profiles.list: List all profiles

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/prabhatdotdev/weave"
	pb "github.com/prabhatdotdev/weave/examples/protobuf/proto"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
	"google.golang.org/protobuf/proto"
)

// In-memory profile store (simulates a database)
var profiles = map[string]*pb.Profile{
	"1": {
		UserId:   "1",
		Bio:      "Software engineer and open source enthusiast",
		Avatar:   "https://example.com/avatars/alice.jpg",
		Location: "San Francisco, CA",
		Website:  "https://alice.dev",
		Verified: true,
	},
	"2": {
		UserId:   "2",
		Bio:      "DevOps engineer specializing in Kubernetes",
		Avatar:   "https://example.com/avatars/bob.jpg",
		Location: "Austin, TX",
		Website:  "https://bob.cloud",
		Verified: true,
	},
	"3": {
		UserId:   "3",
		Bio:      "Full-stack developer building cool things",
		Avatar:   "https://example.com/avatars/charlie.jpg",
		Location: "Seattle, WA",
		Website:  "",
		Verified: false,
	},
}

var server *weave.Server

func main() {
	fmt.Println("=== Weave Protobuf Example - Profile Service ===")
	fmt.Println()

	config := weave.DefaultConfig()
	config.AMQP.Host = getEnv("RABBITMQ_HOST", "localhost")
	config.AMQP.Port = 5672

	var err error
	server, err = weave.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	server.Handle("profiles.get", handleGetProfile)
	server.Handle("profiles.list", handleListProfiles)

	fmt.Println("Registered routes:")
	fmt.Println("  - profiles.get")
	fmt.Println("  - profiles.list")
	fmt.Println()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	fmt.Println("Profile Service started. Press Ctrl+C to stop.")
	if err := server.Start(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	<-ctx.Done()

	fmt.Println("Profile Service stopped.")
}

func handleGetProfile(ctx context.Context, msg *weave.Message) error {
	log.Printf("[profiles.get] Received request (%d bytes)", len(msg.Body))

	var req pb.GetProfileRequest
	requiredErr := errors.New("user_id is required")
	if err := weave.UnmarshalAndValidate(weave.Protobuf, msg, &req, func(req *pb.GetProfileRequest) error {
		if req.UserId == "" {
			return requiredErr
		}
		return nil
	}); err != nil {
		if errors.Is(err, requiredErr) {
			return sendResponse(ctx, msg, &pb.GetProfileResponse{Error: requiredErr.Error()})
		}
		return sendResponse(ctx, msg, &pb.GetProfileResponse{Error: "invalid request format"})
	}

	profile, exists := profiles[req.UserId]
	if !exists {
		log.Printf("[profiles.get] Profile not found for user: %s", req.UserId)
		return sendResponse(ctx, msg, &pb.GetProfileResponse{Error: "profile not found"})
	}

	log.Printf("[profiles.get] Found profile for user: %s", req.UserId)
	return sendResponse(ctx, msg, &pb.GetProfileResponse{Profile: profile})
}

func handleListProfiles(ctx context.Context, msg *weave.Message) error {
	log.Printf("[profiles.list] Received request")

	result := make([]*pb.Profile, 0, len(profiles))
	for _, profile := range profiles {
		result = append(result, profile)
	}

	log.Printf("[profiles.list] Returning %d profiles", len(result))
	return sendResponse(ctx, msg, &pb.ListProfilesResponse{Profiles: result})
}

func sendResponse(ctx context.Context, req *weave.Message, data proto.Message) error {
	if req.ReplyTo == "" {
		return nil
	}

	body, err := proto.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	response := weave.NewMessage(body)
	response.ContentType = "application/x-protobuf"
	response.CorrelationID = req.CorrelationID

	return server.Publish(ctx, req.ReplyTo, response)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
