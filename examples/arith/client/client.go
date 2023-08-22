package main

import (
	"fmt"
	"log"

	pb "github.com/weiwenchen2022/engine/examples/arith/arith"
	"github.com/weiwenchen2022/engine/network/protobuf"
)

func main() {
	client, err := protobuf.Dial("tcp", "localhost:50051")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	// Synchronous call
	args := &pb.Args{A: 7, B: 8}
	reply, err := client.Call("Arith.Multiply", args)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d * %d = %d\n", args.A, args.B, reply.(*pb.Reply).C)

	// Asynchronous call
	divCall := client.Go("Arith.Divide", args, nil)
	replyCall := <-divCall.Done // will be equal to divCall

	if replyCall.Error != nil {
		log.Fatal("arith error:", replyCall.Error)
	}
	quotient := replyCall.Reply.(*pb.Quotient)
	fmt.Printf("Arith: %d / %d = %d, %d\n", args.A, args.B, quotient.Quo, quotient.Rem)
}
