package network

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// ------------------------------------------
// | uint16 big-endian prefix len | payload |
// ------------------------------------------

const MaxBodyLen = 1<<16 - 1

type Conn struct {
	r *bufio.Reader
	n uint16
	w *bufio.Writer
	c io.Closer
}

func NewConn(conn io.ReadWriteCloser) *Conn {
	return &Conn{
		r: bufio.NewReader(conn),
		w: bufio.NewWriter(conn),
		c: conn,
	}
}

func (c *Conn) Read(b []byte) (n int, err error) {
	if c.n == 0 {
		if err = binary.Read(c.r, binary.BigEndian, &c.n); err != nil {
			return
		}
	}

	if len(b) < int(c.n) {
		return 0, io.ErrShortBuffer
	}
	n, c.n = int(c.n), 0
	_, err = io.ReadFull(c.r, b[:n])
	return
}

var ErrMessageTooLarge = errors.New("message too long")

func (c *Conn) Write(b []byte) (n int, err error) {
	n = len(b)
	if n > MaxBodyLen {
		return 0, ErrMessageTooLarge
	}

	if err = binary.Write(c.w, binary.BigEndian, uint16(n)); err != nil {
		return 0, err
	}
	if wn, werr := c.w.Write(b); n != wn {
		n, err = wn, werr
		if err == nil {
			err = io.ErrShortWrite
		}
		return
	}
	return n, c.w.Flush()
}

func (c *Conn) Close() error {
	return c.c.Close()
}

type ConnReader Conn

func (c *ConnReader) Read(b []byte) (int, error) {
	return (*Conn)(c).Read(b)
}

type ConnWriter Conn

func (c *ConnWriter) Write(b []byte) (int, error) {
	return (*Conn)(c).Write(b)
}
