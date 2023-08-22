package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	pb "github.com/weiwenchen2022/engine/examples/helloworld/helloworld"
	"github.com/weiwenchen2022/engine/network"
	"github.com/weiwenchen2022/engine/network/protobuf"
)

var (
	port      = flag.Int("port", 50051, "The server port")
	debugPort = flag.Int("debugPort", 50052, "The server debug port")
)

type Greeter struct{}

func (*Greeter) SayHello(in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("SayHello Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (*Greeter) SayHelloAgain(in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("SayHelloAgain Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello again " + in.GetName()}, nil
}

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := network.NewServer()
	_ = s.Register(&Greeter{})

	// for HTTP debug service
	{
		s.HandleDebugHTTP(network.DefaultDebugPath)

		l, err := net.Listen("tcp", fmt.Sprintf(":%d", *debugPort))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		log.Printf("server debuging at %v", l.Addr())
		go func() { _ = http.Serve(l, nil) }()
	}

	log.Printf("server serving at %v", lis.Addr())
	if err := s.Serve(lis, protobuf.ServeConn); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
