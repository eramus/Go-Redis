//   Copyright 2009-2012 Joubin Houshyar
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package redis

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	//	"os"
	"time"

	"github.com/eramus/worker"
)

const (
	TCP       = "tcp"
	LOCALHOST = "127.0.0.1"
	//	ns1MSec   = 1000000
	////	ns1Sec    = ns1MSec * 1000
	//	ns1Sec    = 1*time.Second
)

// various default sizes for the connections
// exported for user convenience if nedded
const (
	DefaultReqChanSize  = 1000000
	DefaultRespChanSize = 1000000

	DefaultTCPReadBuffSize  = 1024 * 256
	DefaultTCPWriteBuffSize = 1024 * 256
	//	DefaultTCPReadTimeoutNSecs  = ns1Sec * 10
	//	DefaultTCPWriteTimeoutNSecs = ns1Sec * 10
	DefaultTCPReadTimeoutNSecs  = 10 * time.Second
	DefaultTCPWriteTimeoutNSecs = 10 * time.Second
	DefaultTCPLinger            = 0
	DefaultTCPKeepalive         = true
	DefaultHeartbeatSecs        = 1 * time.Second
)

// Redis specific default settings
// exported for user convenience if nedded
const (
	DefaultRedisPassword = ""
	DefaultRedisDB       = 0
	DefaultRedisPort     = 6379
	DefaultRedisHost     = LOCALHOST
)

// ----------------------------------------------------------------------------
// Connection ConnectionSpec
// ----------------------------------------------------------------------------

// Defines the set of parameters that are used by the client connections
//
type ConnectionSpec struct {
	host     string
	port     int
	password string
	db       int
	// tcp specific specs
	rBufSize   int
	wBufSize   int
	rTimeout   time.Duration
	wTimeout   time.Duration
	keepalive  bool
	lingerspec int // -n: finish io; 0: discard, +n: wait for n secs to finish
	// async specs
	reqChanCap int
	rspChanCap int
	//
	heartbeat time.Duration // 0 means no heartbeat
}

// Creates a ConnectionSpec using default settings.
// using the DefaultXXX consts of redis package.
func DefaultSpec() *ConnectionSpec {
	return &ConnectionSpec{
		DefaultRedisHost,
		DefaultRedisPort,
		DefaultRedisPassword,
		DefaultRedisDB,
		DefaultTCPReadBuffSize,
		DefaultTCPWriteBuffSize,
		DefaultTCPReadTimeoutNSecs,
		DefaultTCPWriteTimeoutNSecs,
		DefaultTCPKeepalive,
		DefaultTCPLinger,
		DefaultReqChanSize,
		DefaultRespChanSize,
		DefaultHeartbeatSecs,
	}
}

// Sets the db for connection spec and returns the reference
// Note that you should not this after you have already connected.
func (spec *ConnectionSpec) Db(db int) *ConnectionSpec {
	spec.db = db
	return spec
}

// Sets the host for connection spec and returns the reference
// Note that you should not this after you have already connected.
func (spec *ConnectionSpec) Host(host string) *ConnectionSpec {
	spec.host = host
	return spec
}

// Sets the port for connection spec and returns the reference
// Note that you should not this after you have already connected.
func (spec *ConnectionSpec) Port(port int) *ConnectionSpec {
	spec.port = port
	return spec
}

// Sets the password for connection spec and returns the reference
// Note that you should not this after you have already connected.
func (spec *ConnectionSpec) Password(password string) *ConnectionSpec {
	spec.password = password
	return spec
}

// return the address as string.
func (spec *ConnectionSpec) Heartbeat(period time.Duration) *ConnectionSpec {
	spec.heartbeat = period
	return spec
}

// ----------------------------------------------------------------------------
// Generic Conn handle and methods
// ----------------------------------------------------------------------------

// General control structure used by connections.
//
type connHdl struct {
	spec   *ConnectionSpec
	conn   net.Conn // may want to change this to TCPConn
	reader *bufio.Reader
}

func (chdl *connHdl) String() string {
	return fmt.Sprintf("conn<redis-server@%s>", chdl.conn.RemoteAddr())
}

// Creates and opens a new connection to server per ConnectionSpec.
// The new connection is wrapped by a new connHdl with its bufio.Reader
// delegating to the net.Conn's reader.
//
func newConnHdl(spec *ConnectionSpec) (hdl *connHdl, err Error) {
	here := "newConnHdl"

	if hdl = new(connHdl); hdl == nil {
		return nil, NewError(SYSTEM_ERR, fmt.Sprintf("%s(): failed to allocate connHdl", here))
	}
	addr := fmt.Sprintf("%s:%d", spec.host, spec.port)
	raddr, e := net.ResolveTCPAddr("tcp", addr)
	if e != nil {
		msg := fmt.Sprintf("%s(): failed to resolve remote address %s", here, addr)
		return nil, NewErrorWithCause(SYSTEM_ERR, msg, e)
	}
	conn, e := net.DialTCP(TCP, nil, raddr)
	switch {
	case e != nil:
		err = NewErrorWithCause(SYSTEM_ERR, fmt.Sprintf("%s(): could not open connection", here), e)
		return nil, withError(err)
	case conn == nil:
		err = NewError(SYSTEM_ERR, fmt.Sprintf("%s(): net.Dial returned nil, nil (?)", here))
		return nil, withError(err)
	default:
		configureConn(conn, spec)
		hdl.spec = spec
		hdl.conn = conn
		bufsize := 4096
		hdl.reader = bufio.NewReaderSize(conn, bufsize)
		log.Printf("<INFO> Connected to %s", hdl)
	}
	return hdl, nil
}

func configureConn(conn *net.TCPConn, spec *ConnectionSpec) {
	// these two -- the most important -- are causing problems on my osx/64
	// where a "service unavailable" pops up in the async reads
	// but we absolutely need to be able to use timeouts.
	//			conn.SetReadTimeout(spec.rTimeout);
	//			conn.SetWriteTimeout(spec.wTimeout);
	conn.SetLinger(spec.lingerspec)
	conn.SetKeepAlive(spec.keepalive)
	conn.SetReadBuffer(spec.rBufSize)
	conn.SetWriteBuffer(spec.wBufSize)
}

// onConnect event handler will issue AUTH/SELECT on new connection
// if required.
func (c *connHdl) onConnect() (e Error) {
	if c.spec.password != DefaultRedisPassword {
		_, e = c.ServiceRequest(&AUTH, [][]byte{[]byte(c.spec.password)})
		if e != nil {
			log.Printf("<ERROR> Authentication failed - %s", e.Message())
			return
		}
	}
	if c.spec.db != DefaultRedisDB {
		_, e = c.ServiceRequest(&SELECT, [][]byte{[]byte(fmt.Sprintf("%d", c.spec.db))})
		if e != nil {
			log.Printf("<ERROR> DB Select failed - %s", e.Message())
			return
		}
	}
	return
}

func (c *connHdl) onDisconnect() Error {
	return nil // for now
}

// closes the connHdl's net.Conn connection.
// Is public so that connHdl struct can be used as SyncConnection (TODO: review that.)
//
func (hdl connHdl) Close() error {
	err := hdl.conn.Close()
	if debug() {
		log.Println("[Go-Redis] Closed connection: ", hdl)
	}
	return err
}

// ----------------------------------------------------------------------------
// Connection SyncConnection
// ----------------------------------------------------------------------------

// Defines the service contract supported by synchronous (Request/Reply)
// connections.

type SyncConnection interface {
	ServiceRequest(cmd *Command, args [][]byte) (Response, Error)
	Close() error
}

// Creates a new SyncConnection using the provided ConnectionSpec
func NewSyncConnection(spec *ConnectionSpec) (c SyncConnection, err Error) {
	connHdl, e := newConnHdl(spec)
	if e != nil {
		return nil, e
	}

	e = connHdl.onConnect()
	return connHdl, e
}

// Implementation of SyncConnection.ServiceRequest.
//
func (chdl *connHdl) ServiceRequest(cmd *Command, args [][]byte) (resp Response, err Error) {
	here := "connHdl.ServiceRequest"
	errmsg := ""
	ok := false
	buff, e := CreateRequestBytes(cmd, args) // 2<<<
	if e == nil {
		e = sendRequest(chdl.conn, buff)
		if e == nil {
			resp, e = GetResponse(chdl.reader, cmd)
			if e == nil {
				if resp.IsError() {
					redismsg := fmt.Sprintf(" [%s]: %s", cmd.Code, resp.GetMessage())
					err = NewRedisError(redismsg)
				}
				ok = true
			} else {
				errmsg = fmt.Sprintf("%s(%s): failed to get response", here, cmd.Code)
			}
		} else {
			errmsg = fmt.Sprintf("%s(%s): failed to send request", here, cmd.Code)
		}
	} else {
		errmsg = fmt.Sprintf("%s(%s): failed to create request buffer", here, cmd.Code)
	}

	if !ok {
		return resp, withError(NewErrorWithCause(SYSTEM_ERR, errmsg, e)) // log it on debug
	}

	return
}

// ----------------------------------------------------------------------------
// Asynchronous connections
// ----------------------------------------------------------------------------

const (
	_ worker.StatusCode = iota
	_
	_
	_
	_
	reqerr
	inierr
	snderr
	rcverr
)

// Defines the data corresponding to a requested service call through the
// QueueRequest method of AsyncConnection
// not used yet.
type asyncRequestInfo struct {
	id      int64
	stat    worker.StatusCode
	cmd     *Command
	outbuff *[]byte
	future  interface{}
	error   Error
}
type asyncReqPtr *asyncRequestInfo

// control structure used by asynch connections.
const (
	heartbeatworker int = iota
	manager
	requesthandler
	responsehandler
)

type asyncConnHdl struct {
	worker.Base
	super  *connHdl
	writer *bufio.Writer

	nextid       int64
	pendingReqs  chan asyncReqPtr
	pendingResps chan asyncReqPtr
	faults       chan asyncReqPtr
}

// Creates a new asyncConnHdl with a new connHdl as its delegated 'super'.
// Note it does not start the processing goroutines for the channels.

func newAsyncConnHdl(spec *ConnectionSpec) (async *asyncConnHdl, err Error) {
	//	here := "newAsynConnHDL";
	connHdl, err := newConnHdl(spec)
	if err == nil && connHdl != nil {
		async = new(asyncConnHdl)
		if async != nil {
			async.super = connHdl
			//			var e error
			async.writer = bufio.NewWriterSize(connHdl.conn, spec.wBufSize)

			async.pendingReqs = make(chan asyncReqPtr, spec.reqChanCap)
			async.pendingResps = make(chan asyncReqPtr, spec.rspChanCap)
			async.faults = make(chan asyncReqPtr, spec.reqChanCap) // not sure about sizing here ...

			return
		}
	}
	// fall through here on errors only
	if debug() {
		log.Println("Error creating asyncConnHdl: ", err)
		//		err =  os.NewError("Error creating asyncConnHdl");
	}
	return nil, err
}

// Creates and opens a new AsyncConnection and starts the goroutines for
// request and response processing
// TODO: NewXConnection methods need to return redis.Error due to initial connect
// interaction with redis (AUTH &| SELECT)
func NewAsynchConnection(spec *ConnectionSpec) (conn AsyncConnection, err Error) {
	var async *asyncConnHdl
	if async, err = newAsyncConnHdl(spec); err == nil {
		async.onConnect()
		async.startup()
		go async.Run()
	}
	return async, err
}

// onConnect event handler.
// See connHdl#onConnect
func (c *asyncConnHdl) onConnect() (e Error) {
	return c.super.onConnect()
}

func (c *asyncConnHdl) onDisconnect() (e Error) {
	return
}

var asyncTaskList = []string{
	"heartbeat",
	"request-handler",
	"response-handler",
}

func asyncTask(label string) (int, worker.WorkerTask, worker.StartHook, worker.FinishHook) {
	switch label {
	case "heartbeat":
		return heartbeatworker, heartbeatTask, nil, nil
	case "request-handler":
		return requesthandler, reqProcessingTask, nil, nil
	case "response-handler":
		return responsehandler, rspProcessingTask, nil, nil
	}
	return 0, nil, nil, nil
}

func (c *asyncConnHdl) startup() {

	worker.LoadTasks(c, asyncTaskList, asyncTask)

	log.Println("<INFO> Async connection started")
}

// ----------------------------------------------------------------------------
// aync tasks
// ----------------------------------------------------------------------------

func (c *asyncConnHdl) Run() {
	/* TODO:
	connected:
		clearChannels();
		startWokers();
	disconnected:
		?
	on_fault:
		disconnect();
		goto disconnected;
	on_exit:
		?
	*/
	//	log.Println("MGR: start tasks ...")
	worker.StartTasks(c)
	shutdown := c.GetShutdown()
	status := c.GetStatus()
	//	log.Println("MGR: do task ...")
	for {
		select {
		case stat := <-status:
			log.Println("MGR: Feedback from one of my minions: ", stat)
			// do the shutdown for now -- TODO: try reconnect
			if stat.Event == worker.Faulted {
				log.Println("MGR: Shutting down due to fault in ", stat.Task.Id)
				log.Println("MGR: Signal SHUTDOWN ... ")
				go c.Shutdown()
			} else if stat.Event == worker.Ready {
				log.Println("MGR: Starting ", stat.Task.Name)
				stat.Task.Signal(worker.Start)
			}
		case s := <-shutdown:
			worker.StopTasks(c)
			s <- true
			log.Println("MGR: SHUTDOWN ... ")
		}
	}
}

// Task:
// "One PING only" after receiving a timed tick per ConnectionsSpec period
// - can be interrupted while waiting on ticker
// - uses TryGet on PING response (1 sec as of now)
//
// KNOWN ISSUE:
// possible connection is OK but TryGet timesout and now we just log a warning
// but it is a good measure of latencies in the pipeline and perhaps could be used
// to autoconfigure the params to decrease latencies ... TODO

func heartbeatTask(w worker.Worker, ctl worker.WorkerCtl) (sig *worker.InterruptCode, te *worker.TaskStatus) {
	var c = w.(*asyncConnHdl)
	var async AsyncConnection = c
	select {
	//	case <-NewTimer(ns1Sec * c.super.spec.heartbeat):
	case <-time.NewTimer(c.super.spec.heartbeat).C:
		response, e := async.QueueRequest(&PING, [][]byte{})
		if e != nil {
			return nil, &worker.TaskStatus{reqerr, e}
		}
		stat, re, timedout := response.future.(FutureBool).TryGet(1 * time.Second)
		if re != nil {
			log.Printf("ERROR: Heartbeat recieved error response on PING: %d\n", re)
			return nil, &worker.TaskStatus{worker.Error, re}
		} else if timedout {
			log.Println("Warning: Heartbeat timeout on get PING response.")
		} else {
			// flytrap
			if stat != true {
				log.Println("<BUG> Heartbeat recieved false stat on PING while response error was nil")
				return nil, &worker.TaskStatus{worker.Error, NewError(SYSTEM_ERR, "BUG false stat on PING w/out error")}
			}
		}
	case sig := <-ctl:
		return &sig, &worker.OkStatus
	}
	return nil, &worker.OkStatus
}

// Task:
// process one pending response at a time
// - can be interrupted while waiting on the pending responses queue
// - buffered reader takes care of minimizing network io
//
// KNOWN BUG:
// until we figure out what's the problem with read timeout, can not
// be interrupted if hanging on a read
func rspProcessingTask(w worker.Worker, ctl worker.WorkerCtl) (sig *worker.InterruptCode, te *worker.TaskStatus) {

	var req asyncReqPtr
	var c = w.(*asyncConnHdl)
	select {
	case sig := <-ctl:
		// interrupted
		return &sig, &worker.OkStatus
	case req = <-c.pendingResps:
		// continue to process
	}

	// process response to asyncRequest
	reader := c.super.reader
	cmd := req.cmd

	resp, e3 := GetResponse(reader, cmd)
	if e3 != nil {
		// system error
		log.Println("<TEMP DEBUG> Request sent to faults chan on error in GetResponse: ", e3)
		req.stat = rcverr
		req.error = NewErrorWithCause(SYSTEM_ERR, "GetResponse os.Error", e3)
		c.faults <- req
		return nil, &worker.TaskStatus{rcverr, e3}
	}
	SetFutureResult(req.future, cmd, resp)

	return nil, &worker.OkStatus
}

// Pending further tests, this addresses bug in earlier version
// and can be interrupted

func reqProcessingTask(w worker.Worker, ctl worker.WorkerCtl) (ic *worker.InterruptCode, ts *worker.TaskStatus) {

	var err error
	var errmsg string
	var c = w.(*asyncConnHdl)

	bytecnt := 0
	blen := 0
	bufsize := c.super.spec.wBufSize
	var sig worker.InterruptCode

	select {
	case req := <-c.pendingReqs:
		blen, err := c.processAsyncRequest(req)
		if err != nil {
			errmsg = fmt.Sprintf("processAsyncRequest error in initial phase")
			goto proc_error
		}
		bytecnt += blen
	case sig := <-ctl:
		return &sig, &worker.OkStatus
	}

	for bytecnt < bufsize {
		select {
		case req := <-c.pendingReqs:
			blen, err = c.processAsyncRequest(req)
			if err != nil {
				errmsg = fmt.Sprintf("processAsyncRequest error in batch phase")
				goto proc_error
			}
			if blen > 0 {
				bytecnt += blen
			} else {
				bytecnt = 0
			}

		case sig = <-ctl:
			ic = &sig
			goto done

		default:
			goto done
		}
	}

done:
	c.writer.Flush()
	return ic, &worker.OkStatus

proc_error:
	log.Println(errmsg, err)
	return nil, &worker.TaskStatus{snderr, err}
}

func (c *asyncConnHdl) processAsyncRequest(req asyncReqPtr) (blen int, e error) {
	//	req := <-c.pendingReqs;
	req.id = c.nextId()
	blen = len(*req.outbuff)
	e = sendRequest(c.writer, *req.outbuff)
	if e == nil {
		req.outbuff = nil
		select {
		case c.pendingResps <- req:
		default:
			c.writer.Flush()
			c.pendingResps <- req
			blen = 0
		}
	} else {
		log.Println("<BUG> lazy programmer >> ERROR in processRequest goroutine -req requeued for now")
		// TODO: set stat on future & inform conn control and put it in fauls
		c.pendingReqs <- req
	}
	return
}

// ----------------------------------------------------------------------------
// AsyncConnection interface & Impl
// ----------------------------------------------------------------------------

// Defines the service contract supported by asynchronous (Request/FutureReply)
// connections.

type AsyncConnection interface {
	QueueRequest(cmd *Command, args [][]byte) (*PendingResponse, Error)
}

// Handle to a future response
type PendingResponse struct {
	future interface{} // TODO: stop using runtime casts
}

func (c *asyncConnHdl) QueueRequest(cmd *Command, args [][]byte) (*PendingResponse, Error) {
	shutdown := c.GetShutdown()
	select {
	case s := <-shutdown:
		log.Println("<DEBUG> we're shutdown and not accepting any more requests ...")
		// if this happened, put it back for Run() to take care of
		shutdown <- s
		return nil, NewError(SYSTEM_ERR, "Connection is shutdown.")
	default:
	}

	future := CreateFuture(cmd)

	request := &asyncRequestInfo{0, 0, cmd, nil, future, nil}
	buff, e1 := CreateRequestBytes(cmd, args)
	if e1 == nil {
		request.outbuff = &buff
		c.pendingReqs <- request
	} else {
		errmsg := fmt.Sprintf("Failed to create asynchrequest - %s aborted", cmd.Code)
		request.stat = inierr
		request.error = NewErrorWithCause(SYSTEM_ERR, errmsg, e1) // only makes sense if using go ...
		request.future.(FutureResult).onError(request.error)

		return nil, request.error // remove if restoring go
	}
	//}();
	// done.
	return &PendingResponse{future}, nil
}

// ----------------------------------------------------------------------------
// internal ops
// ----------------------------------------------------------------------------

// request id needs to be unique in context of associated connection
// only one goroutine calls this so no need to provide concurrency guards
func (c *asyncConnHdl) nextId() (id int64) {
	id = c.nextid
	c.nextid++
	return
}

// Either writes all the bytes or it fails and returns an error
//
func sendRequest(w io.Writer, data []byte) (e error) {
	here := "connHdl.sendRequest"
	if w == nil {
		return withNewError(fmt.Sprintf("<BUG> in %s(): nil Writer", here))
	}

	n, e := w.Write(data)
	if e != nil {
		var msg string
		//		switch {
		//		case e == os.EAGAIN:
		//			// socket timeout -- don't handle that yet but may in future ..
		//			msg = fmt.Sprintf("%s(): timeout (os.EAGAIN) error on Write", here)
		//		default:
		// anything else
		msg = fmt.Sprintf("%s(): error on Write", here)
		//		}
		return withOsError(msg, e)
	}

	// doc isn't too clear but the underlying netFD may return n<len(data) AND
	// e == nil, but that's precisely what we're testing for here.
	// presumably we can try sending the remaining bytes but that is precisely
	// what netFD.Write is doing (and it couldn't) so ...
	if n < len(data) {
		msg := fmt.Sprintf("%s(): connection Write wrote %d bytes only.", here, n)
		return withNewError(msg)
	}
	return
}
