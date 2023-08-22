package protobuf

import (
	"errors"
	"testing"

	"github.com/weiwenchen2022/engine/network"
)

type shutdownCodec struct {
	responded chan bool
	closed    bool
}

func (c *shutdownCodec) WriteRequest(*network.Request) error { return nil }
func (c *shutdownCodec) ReadResponse(*network.Response) error {
	c.responded <- true
	return errors.New("shutdownCodec ReadResponseHeader")
}
func (c *shutdownCodec) Close() error {
	c.closed = true
	return nil
}

func TestCloseCodec(t *testing.T) {
	t.Parallel()

	codec := &shutdownCodec{responded: make(chan bool, 1)}
	client := network.NewClientWithCodec(codec)
	<-codec.responded
	client.Close()
	if !codec.closed {
		t.Error("client.Close did not close codec")
	}
}
