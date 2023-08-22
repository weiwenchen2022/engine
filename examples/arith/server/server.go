package main

import (
	"errors"
	"log"
	"net"

	pb "github.com/weiwenchen2022/engine/examples/arith/arith"
	"github.com/weiwenchen2022/engine/network"
	"github.com/weiwenchen2022/engine/network/protobuf"
)

type Arith int

func (*Arith) Multiply(args *pb.Args) (*pb.Reply, error) {
	return &pb.Reply{C: args.A * args.B}, nil
}

func (t *Arith) Divide(args *pb.Args) (*pb.Quotient, error) {
	if args.B == 0 {
		return &pb.Quotient{}, errors.New("divide by zero")
	}
	return &pb.Quotient{Quo: args.A / args.B, Rem: args.A % args.B}, nil
}

func main() {
	s := network.NewServer()
	s.Register(new(Arith))
	l, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	log.Printf("server serving at %v", l.Addr())
	if err := s.Serve(l, protobuf.ServeConn); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
