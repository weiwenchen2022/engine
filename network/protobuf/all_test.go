package protobuf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/weiwenchen2022/engine/network"
	pb "github.com/weiwenchen2022/engine/network/protobuf/protobuf"
	testpb "github.com/weiwenchen2022/engine/network/protobuf/test"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Arith int

func (t *Arith) Add(args *testpb.Args) (*testpb.Reply, error) {
	return &testpb.Reply{C: args.A + args.B}, nil
}

func (t *Arith) Mul(args *testpb.Args) (*testpb.Reply, error) {
	return &testpb.Reply{C: args.A * args.B}, nil
}

func (t *Arith) Div(args *testpb.Args) (*testpb.Reply, error) {
	if args.B == 0 {
		return &testpb.Reply{}, errors.New("divide by zero")
	}
	return &testpb.Reply{C: args.A / args.B}, nil
}

func (t *Arith) Error(*testpb.Args) (*testpb.Reply, error) {
	panic("ERROR")
}

func (t *Arith) SleepMilli(args *testpb.Args) (*testpb.Reply, error) {
	time.Sleep(time.Duration(args.A) * time.Millisecond)
	return &testpb.Reply{}, nil
}

func newServer() *network.Server {
	s := network.NewServer()
	s.Register(new(Arith))
	return s
}

func TestServerNoParams(t *testing.T) {
	t.Parallel()

	cli, srv := net.Pipe()
	defer cli.Close()
	c := network.NewConn(cli)
	go ServeConn(newServer(), srv)
	dec := NewDecoder((*network.ConnReader)(c))
	enc := NewEncoder(((*network.ConnWriter)(c)))

	id, _ := anypb.New(&pb.String{Str: "123"})
	enc.Encode(&pb.Request{Method: "Arith.Add", Id: id})
	var resp pb.Response
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("Decode after no params: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("Expected error, got nil")
	}
}

func TestServerEmptyMessage(t *testing.T) {
	t.Parallel()

	cli, srv := net.Pipe()
	defer cli.Close()
	c := network.NewConn(cli)
	go ServeConn(newServer(), srv)
	dec := NewDecoder((*network.ConnReader)(c))
	enc := NewEncoder((*network.ConnWriter)(c))

	enc.Encode(&pb.Request{})
	var resp pb.Response
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("Decode after empty: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("Expected error, got nil")
	}
}

func TestServer(t *testing.T) {
	t.Parallel()

	cli, srv := net.Pipe()
	defer cli.Close()
	go ServeConn(newServer(), srv)
	c := network.NewConn(cli)
	dec := NewDecoder((*network.ConnReader)(c))
	enc := NewEncoder((*network.ConnWriter)(c))

	// Send hand-coded requests to server, parse responses.
	for i := 0; i < 10; i++ {
		id, _ := anypb.New(&pb.String{Str: string(rune(i))})
		arg, _ := anypb.New(&testpb.Args{A: int64(i), B: int64(i + 1)})
		enc.Encode(&pb.Request{Method: "Arith.Add", Id: id, Arg: arg})
		var resp pb.Response
		err := dec.Decode(&resp)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if resp.Error != nil {
			err, _ := resp.Error.UnmarshalNew()
			t.Fatalf("resp.Error: %s", err)
		}

		x, _ := resp.Id.UnmarshalNew()
		if string(rune(i)) != x.(*pb.String).Str {
			t.Fatalf("resp: bad id %q want %q", x.(*pb.String).Str, string(rune(i)))
		}

		var result testpb.Reply
		resp.Result.UnmarshalTo(&result)
		if int64(2*i+1) != result.C {
			t.Fatalf("resp: bad result: %d + %d = %d", i, i+1, result.C)
		}
	}
}

func TestClient(t *testing.T) {
	t.Parallel()

	// Assume server is okay (TestServer is above).
	// Test client against server.
	cli, srv := net.Pipe()
	go ServeConn(newServer(), srv)

	client := NewClient(cli)
	defer client.Close()

	// Synchronous calls
	args := &testpb.Args{A: 7, B: 8}
	reply, err := client.Call("Arith.Add", args)
	if err != nil {
		t.Errorf("Add: expected no error but got string %q", err)
	}
	if args.A+args.B != reply.(*testpb.Reply).C {
		t.Errorf("Add: got %d expected %d", reply.(*testpb.Reply).C, args.A+args.B)
	}

	args = &testpb.Args{A: 7, B: 8}
	reply, err = client.Call("Arith.Mul", args)
	if err != nil {
		t.Errorf("Mul: expected no error but got string %q", err)
	}
	if args.A*args.B != reply.(*testpb.Reply).C {
		t.Errorf("Mul: got %d expected %d", reply.(*testpb.Reply).C, args.A*args.B)
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
		t.Errorf("Add: got %d expected %d", addReply.C, args.A+args.B)
	}

	mulCall = <-mulCall.Done
	if mulCall.Error != nil {
		t.Errorf("Mul: expected no error but got string %q", mulCall.Error)
	}
	mulReply := mulCall.Reply.(*testpb.Reply)
	if args.A*args.B != mulReply.C {
		t.Errorf("Mul: got %d expected %d", mulReply.C, args.A*args.B)
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
}

func TestMalformedInput(t *testing.T) {
	t.Parallel()

	cli, srv := net.Pipe()
	go cli.Write(append(
		binary.BigEndian.AppendUint16(nil, uint16(len(`{id:1}`))), []byte(`{id:1}`)...)) // invalid protobuf message
	ServeConn(newServer(), srv) // must return, not loop
}

func TestMalformedOutput(t *testing.T) {
	t.Parallel()

	cli, srv := net.Pipe()
	c := network.NewConn(srv)
	id, _ := anypb.New(&pb.Id{Id: 0})
	go NewEncoder((*network.ConnWriter)(c)).Encode(&pb.Response{Id: id})
	go io.ReadAll(c)

	client := NewClient(cli)
	defer client.Close()

	args := &testpb.Args{A: 7, B: 8}
	_, err := client.Call("Arith.Add", args)
	if err == nil {
		t.Error("expected error")
	} else {
		t.Log(err)
	}
}

func TestServerErrorHasNullResult(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	id, _ := anypb.New(&pb.String{Str: "123"})
	b, _ := proto.Marshal(&pb.Request{Method: "Method", Id: id})
	sc := NewServerCodec(struct {
		io.Reader
		io.Writer
		io.Closer
	}{
		Reader: bytes.NewReader(append(binary.BigEndian.AppendUint16(nil, uint16(len(b))), b...)),
		Writer: &out,
		Closer: io.NopCloser(nil),
	})
	r := new(network.Request)
	_ = sc.ReadRequest(r)
	const (
		valueText = "the value we don't want to see"
		errorText = "some error"
	)
	err := sc.WriteResponse(&network.Response{
		ServiceMethod: r.ServiceMethod,
		Seq:           r.Seq,
		Error:         errorText,
		Reply:         &pb.String{Str: valueText},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), errorText) {
		t.Fatalf("Response didn't contain expected error %q: %s", errorText, &out)
	}
	if strings.Contains(out.String(), valueText) {
		t.Errorf("Response contains both an error and value: %s", &out)
	}
}

func TestUnexpectedError(t *testing.T) {
	t.Parallel()

	cli, srv := myPipe()
	go cli.PipeWriter.CloseWithError(errors.New("unexpected error!")) // reader will get this error
	ServeConn(newServer(), srv)                                       // must return, not loop
}

// Copied from package net.
func myPipe() (*pipe, *pipe) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	return &pipe{r1, w2}, &pipe{r2, w1}
}

type pipe struct {
	*io.PipeReader
	*io.PipeWriter
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }

func (pipeAddr) String() string { return "pipe" }

func (p *pipe) Close() error {
	err := p.PipeReader.Close()
	err1 := p.PipeWriter.Close()
	if err == nil {
		err = err1
	}
	return err
}

func (p *pipe) LocalAddr() net.Addr {
	return pipeAddr{}
}

func (p *pipe) RemoteAddr() net.Addr {
	return pipeAddr{}
}

func (*pipe) SetDeadline(time.Time) error {
	return errors.New("pipe does not support deadlines")
}

func (*pipe) SetReadDeadline(time.Time) error {
	return errors.New("pipe does not support deadlines")
}

func (*pipe) SetWriteDeadline(time.Time) error {
	return errors.New("pipe does not support deadlines")
}
