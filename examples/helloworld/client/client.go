package main

import (
	"flag"
	"log"
	"sync"
	"sync/atomic"

	pb "github.com/weiwenchen2022/engine/examples/helloworld/helloworld"
	"github.com/weiwenchen2022/engine/network"
	"github.com/weiwenchen2022/engine/network/protobuf"
)

const defaultName = "world"

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()

	// Set up a connection to the server.
	client, err := protobuf.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer client.Close()

	const MaxCalls = 100
	res := make(chan *network.Call, MaxCalls)
	var recv atomic.Int32
	recv.Add(2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for call := range res {
			if recv.Add(-1) == 0 {
				close(res)
			}

			if call.Error != nil {
				log.Printf("could not greet: %v", call.Error)
				continue
			}

			switch r := call.Reply.(type) {
			default:
				log.Printf("unrecognize reply type: %T", call.Reply)
			case *pb.HelloReply:
				log.Printf("Greeting: %s", r.GetMessage())
			}
		}
		wg.Done()
	}()

	// Contact the server and print out its response.
	client.Go("Greeter.SayHello", &pb.HelloRequest{Name: *name}, res)
	client.Go("Greeter.SayHelloAgain", &pb.HelloRequest{Name: *name}, res)

	wg.Wait()
}
