// Package protobuf implements a ClientCodec and ServerCodec
// for the network package.
package protobuf

import (
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/weiwenchen2022/engine/network"
	pb "github.com/weiwenchen2022/engine/network/protobuf/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type clientCodec struct {
	dec *Decoder // for reading Protobuf messages
	enc *Encoder // for writing Protobuf messages
	c   io.Closer

	// temporary work space
	req  pb.Request
	resp pb.Response

	// responses include the request id but not the request method.
	// Package network expects both.
	// We save the request method in pending when sending a request
	// and then look it up by request ID when filling out the network Response.
	mutex   sync.Mutex        // protects pending
	pending map[uint64]string // map request id to method name
}

// NewClientCodec returns a new network.ClientCodec using ProtoBuf on conn.
func NewClientCodec(conn io.ReadWriteCloser) network.ClientCodec {
	c := network.NewConn(conn)
	return &clientCodec{
		dec:     NewDecoder((*network.ConnReader)(c)),
		enc:     NewEncoder((*network.ConnWriter)(c)),
		c:       c,
		pending: make(map[uint64]string),
	}
}

func (c *clientCodec) WriteRequest(r *network.Request) (err error) {
	c.mutex.Lock()
	c.pending[r.Seq] = r.ServiceMethod
	c.mutex.Unlock()
	defer func() {
		if err != nil {
			c.mutex.Lock()
			delete(c.pending, r.Seq)
			c.mutex.Unlock()
		}
	}()

	c.req.Method = r.ServiceMethod
	c.req.Arg, err = anypb.New(r.Arg.(proto.Message))
	if err != nil {
		return
	}
	c.req.Id, err = anypb.New(&pb.Id{Id: r.Seq})
	if err != nil {
		return err
	}
	return c.enc.Encode(&c.req)
}

func (c *clientCodec) ReadResponse(r *network.Response) error {
	c.resp.Reset()
	if err := c.dec.Decode(&c.resp); err != nil {
		return err
	}

	var id pb.Id
	if err := c.resp.Id.UnmarshalTo(&id); err != nil {
		return err
	}
	c.mutex.Lock()
	r.ServiceMethod = c.pending[id.Id]
	delete(c.pending, id.Id)
	c.mutex.Unlock()

	r.Error = ""
	r.Seq = id.Id
	if c.resp.Error != nil || c.resp.Result == nil {
		var x pb.String
		if err := c.resp.Error.UnmarshalTo(&x); err != nil {
			return fmt.Errorf("invalid error %v", c.resp.Error)
		}
		if x.Str == "" {
			x.Str = "unspecified error"
		}
		r.Error = x.Str
	} else {
		var err error
		if r.Reply, err = c.resp.Result.UnmarshalNew(); err != nil {
			return err
		}
	}
	return nil
}

func (c *clientCodec) Close() error {
	return c.c.Close()
}

// NewClient returns a new network.Client to handle requests to the
// set of services at the other end of the connection.
func NewClient(conn io.ReadWriteCloser) *network.Client {
	return network.NewClientWithCodec(NewClientCodec(conn))
}

// Dial connects to a ProtoBuf server at the specified network address.
func Dial(network, address string) (*network.Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}
