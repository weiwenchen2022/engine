package protobuf

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weiwenchen2022/engine/network"
	testpb "github.com/weiwenchen2022/engine/network/protobuf/test"
)

var (
	server     *network.Server
	serverAddr string
	once       sync.Once
)

func listenTCP() (net.Listener, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0") // any available address
	if err != nil {
		log.Fatalf("net.Listen tcp :0: %v", err)
	}
	return l, l.Addr().String()
}

func startServer() {
	server = network.NewServer()
	server.Register(new(Arith))
	server.RegisterName("network.server.Arith", new(Arith))
	server.RegisterName("server.Arith", new(Arith))

	var l net.Listener
	l, serverAddr = listenTCP()
	log.Println("Test server serving on", serverAddr)
	go server.Serve(l, ServeConn)
}

func TestRPC(t *testing.T) {
	t.Parallel()

	once.Do(startServer)
	testRPC(t, serverAddr)
	testServerRPC(t, serverAddr)
}

func testRPC(t *testing.T, addr string) {
	client, err := Dial("tcp", addr)
	if err != nil {
		t.Fatal("dialing", err)
	}
	defer client.Close()

	// Synchronous calls
	args := &testpb.Args{A: 7, B: 8}
	reply, err := client.Call("Arith.Add", args)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err)
	}
	if args.A+args.B != reply.(*testpb.Reply).C {
		t.Errorf("Add: expected %d got %d", args.A+args.B, reply.(*testpb.Reply).C)
	}

	// Nonexistent method
	args = &testpb.Args{A: 7, B: 0}
	_, err = client.Call("Arith.BadOperation", args)
	// expect an error
	if err == nil {
		t.Error("BadOperation: expected error")
	} else if !strings.Contains(err.Error(), "can't find method ") {
		t.Errorf("BadOperation: expected can't find method error; got %q", err)
	}

	// Unknown service
	args = &testpb.Args{A: 7, B: 8}
	_, err = client.Call("Arith.Unknown", args)
	if err == nil {
		t.Error("expected error calling unknown service")
	} else if !strings.Contains(err.Error(), "method") {
		t.Error("expected error about method; got", err)
	}

	// Out of order.
	args = &testpb.Args{A: 7, B: 8}
	mulCall := client.Go("Arith.Mul", args, nil)
	addCall := client.Go("Arith.Add", args, nil)

	addCall = <-addCall.Done
	if addCall.Error != nil {
		t.Errorf("Add: expected no error but got string %q", addCall.Error)
	}

	addReply := addCall.Reply.(*testpb.Reply)
	if args.A+args.B != addReply.C {
		t.Errorf("Add: expected %d got %d", args.A+args.B, addReply.C)
	}

	mulCall = <-mulCall.Done
	if mulCall.Error != nil {
		t.Errorf("Mul: expected no error but got string %q", mulCall.Error)
	}

	mulReply := mulCall.Reply.(*testpb.Reply)
	if args.A*args.B != mulReply.C {
		t.Errorf("Mul: expected %d got %d", args.A*args.B, mulReply.C)
	}

	// Error test
	args = &testpb.Args{A: 7, B: 0}
	_, err = client.Call("Arith.Div", args)
	// expect an error: zero divide
	if err == nil {
		t.Error("Div: expected error")
	} else if err.Error() != "divide by zero" {
		t.Error("Div: expected divide by zero error; got", err)
	}

	// Bad type.
	_, err = client.Call("Arith.Add", new(testpb.Reply)) // arg would be the correct thing to use
	if err == nil {
		t.Error("expected error calling Arith.Add with wrong arg type")
	} else if !strings.Contains(err.Error(), "type") {
		t.Error("expected error about type; got", err)
	}

	args = &testpb.Args{A: 7, B: 8}
	reply, err = client.Call("Arith.Mul", args)
	if err != nil {
		t.Errorf("Mul: expected no error but got string %q", err)
	}
	if args.A*args.B != reply.(*testpb.Reply).C {
		t.Errorf("Mul: expected %d got %d", args.A*args.B, reply.(*testpb.Reply).C)
	}

	// ServiceName contain "." character
	args = &testpb.Args{A: 7, B: 8}
	reply, err = client.Call("network.server.Arith.Add", args)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err)
	}
	if args.A+args.B != reply.(*testpb.Reply).C {
		t.Errorf("Add: expected %d got %d", args.A+args.B, reply.(*testpb.Reply).C)
	}
}

func testServerRPC(t *testing.T, addr string) {
	client, err := Dial("tcp", addr)
	if err != nil {
		t.Fatal("dialing", err)
	}
	defer client.Close()

	// Synchronous calls
	args := &testpb.Args{A: 7, B: 8}
	reply, err := client.Call("server.Arith.Add", args)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err)
	}
	if args.A+args.B != reply.(*testpb.Reply).C {
		t.Errorf("Add: expected %d got %d", args.A+args.B, reply.(*testpb.Reply).C)
	}
}

type WriteFailCodec struct{}

func (WriteFailCodec) WriteRequest(*network.Request) error {
	// the panic caused by this error used to not unlock a lock.
	return errors.New("fail")
}

func (WriteFailCodec) ReadResponse(*network.Response) error {
	select {}
}

func (WriteFailCodec) Close() error {
	return nil
}

func TestSendDeadlock(t *testing.T) {
	t.Parallel()

	client := network.NewClientWithCodec(WriteFailCodec{})
	defer client.Close()

	done := make(chan bool)
	go func() {
		testSendDeadlock(client)
		testSendDeadlock(client)
		done <- true
	}()
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock")
	case <-done:
		return
	}
}

func testSendDeadlock(client *network.Client) {
	defer func() { recover() }()
	args := &testpb.Args{A: 7, B: 8}
	client.Call("Arith.Add", args)
}

func dialDirect() (*network.Client, error) {
	return Dial("tcp", serverAddr)
}

func countMallocs(dial func() (*network.Client, error), t *testing.T) float64 {
	once.Do(startServer)
	client, err := dial()
	if err != nil {
		t.Fatal("error dialing", err)
	}
	defer client.Close()

	args := &testpb.Args{A: 7, B: 8}
	// reply := new(Reply)
	return testing.AllocsPerRun(100, func() {
		reply, err := client.Call("Arith.Add", args)
		if err != nil {
			t.Errorf("Add: expected no error but got string %q", err)
		}
		if args.A+args.B != reply.(*testpb.Reply).C {
			t.Errorf("Add: expected %d got %d", args.A+args.B, reply.(*testpb.Reply).C)
		}
	})
}

func TestCountMallocs(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping malloc count in short mode")
	}
	if runtime.GOMAXPROCS(0) > 1 {
		t.Skip("skipping; GOMAXPROCS>1")
	}
	fmt.Printf("mallocs per rpc round trip: %v\n", countMallocs(dialDirect, t))
}

type writeCrasher struct {
	done chan struct{}
}

func (w *writeCrasher) Read([]byte) (int, error) {
	<-w.done
	return 0, io.EOF
}

func (writeCrasher) Write([]byte) (int, error) {
	return 0, errors.New("fake write failure")
}

func (writeCrasher) Close() error {
	return nil
}

func TestClientWriteError(t *testing.T) {
	t.Parallel()

	w := &writeCrasher{done: make(chan struct{})}
	c := NewClient(w)
	defer c.Close()

	_, err := c.Call("foo", &testpb.Args{})
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "fake write failure" {
		t.Error("unexpected value of error:", err)
	}
	close(w.done)
}

func TestTCPClose(t *testing.T) {
	t.Parallel()

	once.Do(startServer)

	client, err := dialDirect()
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer client.Close()

	args := &testpb.Args{A: 17, B: 8}
	reply, err := client.Call("Arith.Mul", args)
	if err != nil {
		t.Fatal("arith error:", err)
	}
	t.Logf("Arith: %d * %d = %d\n", args.A, args.B, reply.(*testpb.Reply).C)
	if args.A*args.B != reply.(*testpb.Reply).C {
		t.Errorf("Add: expected %d got %d", args.A*args.B, reply.(*testpb.Reply).C)
	}
}

func TestErrorAfterClientClose(t *testing.T) {
	t.Parallel()

	once.Do(startServer)

	client, err := dialDirect()
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal("close error:", err)
	}
	_, err = client.Call("Arith.Add", &testpb.Args{A: 7, B: 9})
	if network.ErrShutdown != err {
		t.Errorf("Forever: expected ErrShutdown got %v", err)
	}
}

func TestServeExitAfterListenerClose(t *testing.T) {
	t.Parallel()

	newServer := network.NewServer()
	newServer.Register(new(Arith))
	newServer.RegisterName("network.newServer.Arith", new(Arith))
	newServer.RegisterName("newServer.Arith", new(Arith))

	var l net.Listener
	l, _ = listenTCP()
	l.Close()
	newServer.Serve(l, ServeConn)
}

func TestShutdown(t *testing.T) {
	t.Parallel()

	var l net.Listener
	l, _ = listenTCP()
	ch := make(chan net.Conn, 1)
	go func() {
		defer l.Close()
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
		}
		ch <- c
	}()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c1 := <-ch
	if c1 == nil {
		t.Fatal(err)
	}

	newServer := network.NewServer()
	newServer.Register(new(Arith))
	go ServeConn(newServer, c1)

	args := &testpb.Args{A: 7, B: 8}
	// reply := new(Reply)
	client := NewClient(c)
	_, err = client.Call("Arith.Add", args)
	if err != nil {
		t.Fatal(err)
	}

	// On an unloaded system 10ms is usually enough to fail 100% of the time
	// with a broken server. On a loaded system, a broken server might incorrectly
	// be reported as passing, but we're OK with that kind of flakiness.
	// If the code is correct, this test will never fail, regardless of timeout.
	args.A = 10 // 10 ms
	done := make(chan *network.Call, 1)
	call := client.Go("Arith.SleepMilli", args, done)
	c.(*net.TCPConn).CloseWrite()
	<-done
	if call.Error != nil {
		t.Fatal(err)
	}
}

func benchmarkEndToEnd(b *testing.B, dial func() (*network.Client, error)) {
	once.Do(startServer)
	client, err := dial()
	if err != nil {
		b.Fatal("error dialing:", err)
	}
	defer client.Close()

	// Synchronous calls
	args := &testpb.Args{A: 7, B: 8}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reply, err := client.Call("Arith.Add", args)
			if err != nil {
				b.Fatalf("rpc error: Add: expected no error but got string %q", err)
			}
			if args.A+args.B != reply.(*testpb.Reply).C {
				b.Fatalf("rpc error: Add: expected %d got %d", args.A+args.B, reply.(*testpb.Reply).C)
			}
		}
	})
}

func benchmarkEndToEndAsync(b *testing.B, dial func() (*network.Client, error)) {
	if b.N == 0 {
		return
	}

	const MaxConcurrentCalls = 100

	once.Do(startServer)

	client, err := dial()
	if err != nil {
		b.Fatal("error dialing:", err)
	}
	defer client.Close()

	// Asynchronous calls
	args := &testpb.Args{A: 7, B: 8}

	var send atomic.Int32
	send.Store(int32(b.N))
	var recv atomic.Int32
	recv.Store(int32(b.N))

	procs := 4 * runtime.GOMAXPROCS(-1)
	var wg sync.WaitGroup
	wg.Add(procs)

	gate := make(chan bool, MaxConcurrentCalls)
	res := make(chan *network.Call, MaxConcurrentCalls)
	b.ResetTimer()

	for p := 0; p < procs; p++ {
		go func() {
			for send.Add(-1) >= 0 {
				gate <- true
				client.Go("Arith.Add", args, res)
			}
		}()
		go func() {
			for call := range res {
				A := call.Arg.(*testpb.Args).A
				B := call.Arg.(*testpb.Args).B
				C := call.Reply.(*testpb.Reply).C
				if A+B != C {
					b.Errorf("incorrect reply: Add: expected %d got %d", A+B, C)
					return
				}
				<-gate
				if recv.Add(-1) == 0 {
					close(res)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkEndToEnd(b *testing.B) {
	benchmarkEndToEnd(b, dialDirect)
}

func BenchmarkEndToEndAsync(b *testing.B) {
	benchmarkEndToEndAsync(b, dialDirect)
}
