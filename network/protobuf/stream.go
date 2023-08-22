package protobuf

import (
	"io"

	"github.com/weiwenchen2022/engine/network"
	"google.golang.org/protobuf/proto"
)

const defaultBufSize = 256

// A Decoder reads and decodes Proto Buffer values from an input stream.
type Decoder struct {
	r    *network.ConnReader
	data []byte
}

// NewDecoder returns a new decoder that reads from r.
//
// The decoder introduces its own buffering and may
// read data from r beyond the Proto Buffer values requested.
func NewDecoder(r *network.ConnReader) *Decoder {
	return &Decoder{
		r:    r,
		data: make([]byte, 4096),
	}
}

func (dec *Decoder) Decode(m proto.Message) (err error) {
	var n int
	for err == nil {
		n, err = dec.r.Read(dec.data)
		if err == nil {
			break
		}
		if io.ErrShortBuffer == err {
			l := len(dec.data) * 2
			if l == 0 {
				l = defaultBufSize
			}
			dec.data = make([]byte, l)
			err = nil
			continue
		}
		break
	}
	if err != nil {
		return
	}
	return proto.Unmarshal(dec.data[:n], m)
}

// An Encoder writes Proto Buffer values to an output stream.
type Encoder struct {
	w *network.ConnWriter
}

// NewEncoder returns a new encoder that writes to w.
func NewEncoder(w *network.ConnWriter) *Encoder {
	return &Encoder{w: w}
}

// Encode writes the Proto Buffer encoding of v to the stream.
// preceded by big-endian uint16 len.
func (enc *Encoder) Encode(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	_, err = enc.w.Write(b)
	return err
}
