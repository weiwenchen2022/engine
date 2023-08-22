package protobuf

import (
	"errors"
	"io"
	"sync"

	"github.com/weiwenchen2022/engine/network"
	pb "github.com/weiwenchen2022/engine/network/protobuf/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var errMissingArg = errors.New("protobuf: request body missing arg")

type serverCodec struct {
	dec *Decoder // for reading Protobuf messages
	enc *Encoder // for writing Protobuf messages
	c   io.Closer

	// temporary work space
	req pb.Request

	// Protobuf clients can use arbitrary values as request IDs.
	// Package network expects uint64 request IDs.
	// We assign uint64 sequence numbers to incoming requests
	// but save the original request ID in the pending map.
	// When responds, we use the sequence number in
	// the response to find the original request ID.
	mutex   sync.Mutex // protects seq, pending
	seq     uint64
	pending map[uint64]*anypb.Any
}

// NewServerCodec returns a new network.ServerCodec using Protobuf on conn.
func NewServerCodec(conn io.ReadWriteCloser) network.ServerCodec {
	c := network.NewConn(conn)
	return &serverCodec{
		dec:     NewDecoder((*network.ConnReader)(c)),
		enc:     NewEncoder((*network.ConnWriter)(c)),
		c:       c,
		pending: make(map[uint64]*anypb.Any),
	}
}

type temporaryError struct {
	error
}

func (temporaryError) Temporary() bool {
	return true
}

func (c *serverCodec) ReadRequest(r *network.Request) error {
	c.req.Reset()
	if err := c.dec.Decode(&c.req); err != nil {
		return err
	}

	r.ServiceMethod = c.req.Method

	// Request id can be any protobuf value;
	// network package expects uint64. Translate to
	// internal uint64 and save JSON on the side.
	c.mutex.Lock()
	c.seq++
	c.pending[c.seq] = c.req.Id
	c.req.Id = nil
	r.Seq = c.seq
	c.mutex.Unlock()

	if c.req.Arg == nil {
		return temporaryError{errMissingArg}
	}

	var err error
	if r.Arg, err = c.req.Arg.UnmarshalNew(); err != nil {
		return temporaryError{err}
	}
	return nil
}

var (
	null, _ = anypb.New(&pb.Null{})

	// A value sent as a placeholder for the server's response value when the server
	// receives an invalid request. It is never decoded by the client since the Response
	// contains an error when it is used.
	invalidRequest, _ = anypb.New(&pb.InvalidRequest{})
)

func (c *serverCodec) WriteResponse(r *network.Response) error {
	c.mutex.Lock()
	b, ok := c.pending[r.Seq]
	if !ok {
		c.mutex.Unlock()
		return errors.New("invalid sequence number in response")
	}
	delete(c.pending, r.Seq)
	c.mutex.Unlock()

	if b == nil {
		// Invalid request so no id. Use null.
		b = null
	}

	resp := &pb.Response{Id: b}
	var err error
	if r.Error == "" {
		resp.Result, err = anypb.New(r.Reply.(proto.Message))
	} else {
		resp.Error, err = anypb.New(&pb.String{Str: r.Error})
	}
	if err != nil {
		return err
	}
	return c.enc.Encode(resp)
}

func (c *serverCodec) Close() error {
	return c.c.Close()
}

// ServeConn runs the Protobuf server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
func ServeConn(server *network.Server, conn io.ReadWriteCloser) {
	server.ServeCodec(NewServerCodec(conn))
}
