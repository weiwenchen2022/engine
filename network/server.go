/*
Package network provides access to the exported methods of an object across a
network or other I/O connection.  A server registers an object, making it visible
as a service with the name of the type of the object.  After registration, exported
methods of the object will be accessible remotely.  A server may register multiple
objects (services) of different types but it is an error to register multiple
objects of the same type.

Only methods that satisfy these criteria will be made available for remote access;
other methods will be ignored:

  - the method's type is exported.
  - the method is exported.
  - the method has one argument, a pointer to exported type.
  - the method has two returns, a pointer to exported type and type error.

In effect, the method must look schematically like

	func (*Service) Method(*ArgType) (*ReplyType, error)

The method's argument represents the arguments provided by the caller; the
first return represents the result to be returned to the caller.
The method's error return value, if non-nil, is passed back as a string that the client
sees as if created by errors.New.  If an error is returned, the reply return
will not be sent back to the client.

The server may handle requests on a single connection by calling ServeCodec.  More
typically it will create a network listener and call Serve.

A client wishing to use the service establishes a connection and then invokes
NewClientWithCodec on the connection.  The resulting Client object has two methods,
Call and Go, that specify the service and method to call, a pointer containing the arguments.

The Call method waits for the remote call to complete while the Go method
launches the call asynchronously and signals completion using the Call
structure's Done channel.

Package protobuf is used to transport the data.

Here is a simple example.  A server wishes to export an object of type Arith:

First create a arith.proto file:

	syntax = "proto3";
	package arith;

	option go_package = "github.com/weiwenchen2022/engine/examples/arith/arith";

	message Args {
		int64 a = 1;
	    int64 b = 2;
	}

	message Reply {
		int64 c = 1;
	}

	message Quotient {
		int64 quo = 1;
		int64 rem = 2;
	}

Second using the protocol buffer compiler `protoc` with the protocol compiler plugins for Go to generate "arith.pb.go" file:

Install the protocol compiler plugins for Go using the following commands:

```shell
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

Run the following command:

```shell
$ protoc --go_out=. --go_opt=paths=source_relative arith/arith.proto
```

This will regenerate the arith/arith.pb.go file.

The server calls (for TCP service):

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

	s := network.NewServer()
	s.Register(new(Arith))
	l, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	s.Serve(l, protobuf.ServeConn)

At this point, clients can see a service "Arith" with methods "Arith.Multiply" and
"Arith.Divide".  To invoke one, a client first dials the server:

	client, err := protobuf.Dial("tcp", "localhost:50051")
	if err != nil {
		log.Fatal("dialing:", err)
	}

Then it can make a remote call:

	// Synchronous call
	args := &pb.Args{A:7, B: 8}
	reply, err := client.Call("Arith.Multiply", args)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d * %d = %d\n", args.A, args.B, reply.(*pb.Reply).C)

or

	// Asynchronous call
	divCall := client.Go("Arith.Divide", args, nil)
	replyCall := <-divCall.Done	// will be equal to divCall
	// check errors, print, etc.

A server implementation will often provide a simple, type-safe wrapper for the
client.
*/
package network

import (
	"errors"
	"fmt"
	"go/token"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultDebugPath used by HandleDebugHTTP
const DefaultDebugPath = "/debug/network"

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type

	numCalls atomic.Uint64
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

// Request written for every call. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Request struct {
	ServiceMethod string // format: "Service.Method"
	Seq           uint64 // sequence number chosen by client
	Arg           any

	next *Request // for free list in Server
}

// Response written for every return. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Response struct {
	ServiceMethod string // echoes that of the Request
	Seq           uint64 // echoes that of the request
	Reply         any
	Error         string // error, if any.

	next *Response // for free list in Server
}

// Server represents a Server.
type Server struct {
	serviceMap sync.Map // map[string]*service

	reqLock  sync.Mutex // protects freeReq
	freeReq  *Request
	respLock sync.Mutex // protects freeResp
	freeResp *Response
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{}
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for reflect.Pointer == t.Kind() {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return token.IsExported(t.Name()) || t.PkgPath() == ""
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//   - exported method of exported type
//   - one argument, a poiner to exported type
//   - two return values, a poiner to exported type and type error
//
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (server *Server) Register(rcvr any) error {
	return server.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (server *Server) RegisterName(name string, rcvr any) error {
	return server.register(rcvr, name, true)
}

// logRegisterError specifies whether to log problems during method registration.
// To debug registration, recompile the package with this set to true.
const logRegisterError = false

func (server *Server) register(rcvr any, name string, useName bool) error {
	typ := reflect.TypeOf(rcvr)
	rcvrv := reflect.ValueOf(rcvr)
	sname := name
	if !useName {
		sname = reflect.Indirect(rcvrv).Type().Name()
	}
	if sname == "" {
		s := "network.Register: no service name for type " + typ.String()
		log.Print(s)
		return errors.New(s)
	}
	if !useName && !token.IsExported(sname) {
		s := "network.Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}

	s := &service{
		name: sname,
		rcvr: rcvrv,
		typ:  typ,
	}

	// Install the methods
	s.method = suitableMethods(typ, logRegisterError)

	if len(s.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PointerTo(typ), false)
		if len(method) != 0 {
			str = "network.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "network.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Print(str)
		return errors.New(str)
	}

	if _, dup := server.serviceMap.LoadOrStore(sname, s); dup {
		return errors.New("network: service already defined: " + sname)
	}
	return nil
}

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// suitableMethods returns suitable Rpc methods of typ. It will log
// errors if logErr is true.
func suitableMethods(typ reflect.Type, logErr bool) map[string]*methodType {
	methods := make(map[string]*methodType, typ.NumMethod())
	for m, n := 0, typ.NumMethod(); m < n; m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name

		// Method must be exported.
		if !method.IsExported() {
			continue
		}

		// Method needs two ins: receiver, *arg.
		if numIn := mtype.NumIn(); numIn != 2 {
			if logErr {
				log.Printf("network.Register: method %q has %d input parameters; needs exactly two\n", mname, numIn)
			}
			continue
		}

		argType := mtype.In(1)
		// Arg must be a pointer.
		if reflect.Pointer != argType.Kind() {
			if logErr {
				log.Printf("network.Register: arg type of method %q is not a pointer: %q\n", mname, argType)
			}
			continue
		} else if !isExportedOrBuiltinType(argType) {
			// Arg type must be exported.
			if logErr {
				log.Printf("network.Register: arg type of method %q is not exported: %q\n", mname, argType)
			}
			continue
		}

		// Method needs two outs: *reply, error.
		if numOut := mtype.NumOut(); numOut != 2 {
			if logErr {
				log.Printf("network.Register: method %q has %d output parameters; needs exactly two\n", mname, numOut)
			}
			continue
		}

		replyType := mtype.Out(0)
		// The reply must be a pointer.
		if reflect.Pointer != replyType.Kind() {
			if logErr {
				log.Printf("network.Register: reply type of method %q is not a pointer: %q\n", mname, replyType)
			}
			continue
		} else if !isExportedOrBuiltinType(replyType) {
			// Reply type must be exported.
			if logErr {
				log.Printf("network.Register: reply type of method %q is not exported: %q\n", mname, replyType)
			}
			continue
		}

		// The Second return type of the method must be error.
		if returnType := mtype.Out(1); typeOfError != returnType {
			if logErr {
				log.Printf("network.Register: return type of method %q is %q, must be error\n", mname, returnType)
			}
			continue
		}

		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// A value sent as a placeholder for the server's response value when the server
// receives an invalid request. It is never decoded by the client since the Response
// contains an error when it is used.
var invalidRequest any

func (server *Server) sendResponse(sending *sync.Mutex, req *Request, reply any, codec ServerCodec, errmsg string) {
	resp := server.getResponse()
	// Encode the response
	resp.ServiceMethod = req.ServiceMethod
	resp.Seq = req.Seq
	if errmsg != "" {
		resp.Error = errmsg
	} else {
		resp.Reply = reply
	}
	sending.Lock()
	err := codec.WriteResponse(resp)
	if debugLog && err != nil {
		log.Println("network: writing response:", err)
	}
	sending.Unlock()
	server.freeResponse(resp)
}

func (m *methodType) NumCalls() uint64 {
	return m.numCalls.Load()
}

func (s *service) call(server *Server, sending *sync.Mutex, wg *sync.WaitGroup, mtype *methodType, req *Request, codec ServerCodec) {
	defer wg.Done()
	mtype.numCalls.Add(1)
	function := mtype.method.Func
	argv := reflect.ValueOf(req.Arg)

	reply, err := func() (reply any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
				reply = nil
			}
		}()
		// Invoke the method.
		returnValues := function.Call([]reflect.Value{s.rcvr, argv})
		// The return value for the method are a reply and an error.
		replyv, errInter := returnValues[0], returnValues[1].Interface()
		if errInter != nil {
			return nil, errInter.(error)
		}
		return replyv.Interface(), nil
	}()
	errmsg := ""
	if err != nil {
		errmsg = err.Error()
	}
	server.sendResponse(sending, req, reply, codec, errmsg)
	server.freeRequest(req)
}

// ServeCodec runs the server on a signle connection uses the specified codec to
// decode requests and encode responses.
// ServeCodec blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeCodec in a go statement.
// See NewClientWithCodec's comment for information about concurrent access.
func (server *Server) ServeCodec(codec ServerCodec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		service, mtype, req, keepReading, err := server.readRequest(codec)
		if err != nil {
			if debugLog && io.EOF != err {
				log.Println("network:", err)
			}
			if !keepReading {
				break
			}
			// send a response if we actually managed to read a header.
			if req != nil {
				server.sendResponse(sending, req, invalidRequest, codec, err.Error())
				server.freeRequest(req)
			}
			continue
		}
		wg.Add(1)
		go service.call(server, sending, wg, mtype, req, codec)
	}
	// We've seen that there are no more requests.
	// Wait for responses to be sent before closing codec.
	wg.Wait()
	codec.Close()
}

func (server *Server) getRequest() *Request {
	server.reqLock.Lock()
	req := server.freeReq
	if req == nil {
		req = new(Request)
	} else {
		server.freeReq = req.next
		*req = Request{}
	}
	server.reqLock.Unlock()
	return req
}

func (server *Server) freeRequest(req *Request) {
	server.reqLock.Lock()
	req.next = server.freeReq
	server.freeReq = req
	server.reqLock.Unlock()
}

func (server *Server) getResponse() *Response {
	server.respLock.Lock()
	resp := server.freeResp
	if resp == nil {
		resp = new(Response)
	} else {
		server.freeResp = resp.next
		*resp = Response{}
	}
	server.respLock.Unlock()
	return resp
}

func (server *Server) freeResponse(resp *Response) {
	server.respLock.Lock()
	resp.next = server.freeResp
	server.freeResp = resp
	server.respLock.Unlock()
}

type temporary interface {
	Temporary() bool
}

func (server *Server) readRequest(codec ServerCodec) (svc *service, mtype *methodType, req *Request, keepReading bool, err error) {
	// Grab the request.
	req = server.getRequest()
	// Assume we read the header successfully. If we see an error now,
	// we can still recover and move on to the next request.
	keepReading = true
	err = codec.ReadRequest(req)
	if err != nil {
		t, ok := err.(temporary)
		keepReading = ok && t.Temporary()
		if io.EOF == err || io.ErrUnexpectedEOF == err {
			return
		}
		err = errors.New("network: server cannot decode request: " + err.Error())
		return
	}

	dot := strings.LastIndex(req.ServiceMethod, ".")
	if dot < 0 {
		err = errors.New("network: service/method request ill-formed: " + req.ServiceMethod)
		return
	}
	serviceName := req.ServiceMethod[:dot]
	methodName := req.ServiceMethod[dot+1:]

	// Look up the request.
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("network: can't find service " + req.ServiceMethod)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("network: can't find method " + req.ServiceMethod)
	}
	return
}

// Serve accepts connections on the listener and serves requests
// for each incoming connection. Serve blocks until the listener
// returns a non-nil error. The caller typically invokes Serve in a
// go statement.
func (server *Server) Serve(lis net.Listener, serveConn func(*Server, io.ReadWriteCloser)) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Print("network.Serve: accept:", err)
			return err
		}
		go serveConn(server, conn)
	}
}

// A ServerCodec implements reading of requests and writing of
// responses for the server side of an session.
// The server calls ReadRequest to read requests from the connection,
// and it calls WriteResponse to write a response back.
// The server calls Close when finished with the connection.
// See NewClientWithCodec's comment for information about concurrent access.
type ServerCodec interface {
	ReadRequest(*Request) error
	WriteResponse(*Response) error

	// Close can be called multiple times and must be idempotent.
	Close() error
}

// HandleDebugHTTP registers a debugging handler on debugPath.
// It is still necessary to invoke http.Serve(), typically in a go statement.
func (server *Server) HandleDebugHTTP(debugPath string) {
	http.Handle(debugPath, debugHTTP{server})
}
