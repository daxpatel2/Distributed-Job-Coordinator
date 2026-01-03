package main

import (
	"flag"
	"fmt"

	coordv1 "example.com/mini-coordinator/gen/example.com/mini-coordinator/gen/coordv1"
	psv1 "example.com/mini-coordinator/gen/example.com/mini-coordinator/gen/psv1"
	"example.com/mini-coordinator/internal/coord"
	"example.com/mini-coordinator/internal/ps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"log"
	"net"
)

func main() {
	// Network initialization
	var addr string
	//define command line flags that will help us run this more effectively as this runs in the cmd
	// grab the flags from cmd and save them to address of addr, default is 127.0.0.1:50051,
	flag.StringVar(&addr, "addr", "127.0.0.1:50051", "listen address")
	// must parse flags
	flag.Parse()

	//create the listener
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// engine startup
	// create an empty 'dispatch center'
	grpcServer := grpc.NewServer()

	coordImpl := coord.NewCoordinator()

	coordv1.RegisterCoordinatorServer(grpcServer, coordImpl)

	psImpl := ps.NewServer()
	psv1.RegisterParameterServerServer(grpcServer, psImpl)

	reflection.Register(grpcServer) // developer feature to self-describe

	stopCh := make(chan struct{})
	go coordImpl.RunReaper(stopCh)

	fmt.Printf("coordd listening on %s\n", addr)
	// this is an infinite loop, the server is listening continuously until I cant
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
