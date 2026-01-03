package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	coordv1 "example.com/mini-coordinator/mini-coordinator/gen/example.com/mini-coordinator/gen/coordv1"
	psv1 "example.com/mini-coordinator/mini-coordinator/gen/example.com/mini-coordinator/gen/psv1"
	"example.com/mini-coordinator/mini-coordinator/internal/coord"
	"example.com/mini-coordinator/mini-coordinator/internal/ps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// main is the entry point for the Coordinator daemon (coordd).
// It initializes the gRPC server, registers the Coordinator and ParameterServer services,
// and starts listening for connections.
func main() {
	// Network initialization
	var addr string
	// Define command line flags to configure the listener address.
	// Default is 127.0.0.1:50051.
	flag.StringVar(&addr, "addr", "127.0.0.1:50051", "listen address")
	flag.Parse()

	// Create the TCP listener.
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Engine startup
	// Create a new gRPC server instance.
	grpcServer := grpc.NewServer()

	// Initialize and register the Coordinator service.
	coordImpl := coord.NewCoordinator()
	coordv1.RegisterCoordinatorServer(grpcServer, coordImpl)

	// Initialize and register the Parameter Server service.
	psImpl := ps.NewServer()
	psv1.RegisterParameterServerServer(grpcServer, psImpl)

	// Register reflection service on gRPC server.
	// This allows tools like grpcurl to inspect the server's API at runtime.
	reflection.Register(grpcServer)

	// Start the background reaper process for failure detection.
	stopCh := make(chan struct{})
	go coordImpl.RunReaper(stopCh)

	fmt.Printf("coordd listening on %s\n", addr)

	// Start serving requests. This blocks until the server stops.
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
