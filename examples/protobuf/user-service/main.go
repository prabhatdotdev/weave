package main

// User Service - Server + Client Weave example with Protocol Buffers.
//
// This service demonstrates the combined server/client pattern using protobuf:
//   - Acts as a SERVER to handle incoming user requests
//   - Acts as a CLIENT to call the Profile Service
//
// Routes:
//   - users.get: Get a user by ID (enriched with profile)
//   - users.list: List all users (enriched with profiles)

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prabhatdotdev/weave"
	pb "github.com/prabhatdotdev/weave/examples/protobuf/proto"
	_ "github.com/prabhatdotdev/weave/transport/amqp"
	"google.golang.org/protobuf/proto"
)

var users = map[string]*pb.User{
	"1": {Id: "1", Name: "Alice Smith", Email: "alice@example.com"},
	"2": {Id: "2", Name: "Bob Johnson", Email: "bob@example.com"},
	"3": {Id: "3", Name: "Charlie Brown", Email: "charlie@example.com"},
}

var (
	server        *weave.Server
	profileClient *weave.Client
)

func main() {
	fmt.Println("=== Weave Protobuf Example - User Service ===")
	fmt.Println()

	config := weave.DefaultConfig()
	config.AMQP.Host = getEnv("RABBITMQ_HOST", "localhost")
	config.AMQP.Port = 5672

	var err error
	profileClient, err = weave.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer profileClient.Close()

	server, err = weave.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	server.Handle("users.get", handleGetUser)
	server.Handle("users.list", handleListUsers)

	fmt.Println("Registered routes:")
	fmt.Println("  - users.get")
	fmt.Println("  - users.list")
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

	fmt.Println("Connecting to Profile Service...")
	if err := profileClient.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect client: %v", err)
	}
	fmt.Println("Connected!")
	fmt.Println()

	fmt.Println("User Service started. Press Ctrl+C to stop.")
	if err := server.Start(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	<-ctx.Done()

	fmt.Println("User Service stopped.")
}

func handleGetUser(ctx context.Context, msg *weave.Message) error {
	log.Printf("[users.get] Received request (%d bytes)", len(msg.Body))

	var req pb.GetUserRequest
	requiredErr := errors.New("id is required")
	if err := weave.UnmarshalAndValidate(weave.Protobuf, msg, &req, func(req *pb.GetUserRequest) error {
		if req.Id == "" {
			return requiredErr
		}
		return nil
	}); err != nil {
		if errors.Is(err, requiredErr) {
			return sendResponse(ctx, msg, &pb.GetUserResponse{Error: requiredErr.Error()})
		}
		return sendResponse(ctx, msg, &pb.GetUserResponse{Error: "invalid request format"})
	}

	user, exists := users[req.Id]
	if !exists {
		log.Printf("[users.get] User not found: %s", req.Id)
		return sendResponse(ctx, msg, &pb.GetUserResponse{Error: "user not found"})
	}

	enriched := &pb.EnrichedUser{Id: user.Id, Name: user.Name, Email: user.Email}

	profile, err := getProfile(ctx, user.Id)
	if err != nil {
		log.Printf("[users.get] Warning: could not fetch profile for user %s: %v", user.Id, err)
	} else {
		enriched.Profile = profile
	}

	log.Printf("[users.get] Returning user: %s", user.Id)
	return sendResponse(ctx, msg, &pb.GetUserResponse{User: enriched})
}

func handleListUsers(ctx context.Context, msg *weave.Message) error {
	log.Printf("[users.list] Received request")

	result := make([]*pb.EnrichedUser, 0, len(users))

	var wg sync.WaitGroup
	var mu sync.Mutex
	profilesMap := make(map[string]*pb.Profile)

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

	for _, user := range users {
		enriched := &pb.EnrichedUser{
			Id:      user.Id,
			Name:    user.Name,
			Email:   user.Email,
			Profile: profilesMap[user.Id],
		}
		result = append(result, enriched)
	}

	log.Printf("[users.list] Returning %d users", len(result))
	return sendResponse(ctx, msg, &pb.ListUsersResponse{Users: result})
}

func getProfile(ctx context.Context, userID string) (*pb.Profile, error) {
	reqData, err := proto.Marshal(&pb.GetProfileRequest{UserId: userID})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	response, err := profileClient.Call(ctx, "profiles.get",
		weave.NewMessage(reqData),
		weave.WithTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("call failed: %w", err)
	}

	var resp pb.GetProfileResponse
	if err := proto.Unmarshal(response.Body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("profile service error: %s", resp.Error)
	}

	return resp.Profile, nil
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
