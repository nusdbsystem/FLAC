package coordinator

import (
	"FLAC/configs"
	"FLAC/network"
	"bufio"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"
)

type Commu struct {
	done     chan bool
	listener net.Listener
	stmt     *Context
	connMap  *sync.Map
}

func NewConns(stmt *Context, address string) *Commu {
	res := &Commu{stmt: stmt}
	res.connMap = &sync.Map{}
	res.done = make(chan bool, 1)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	configs.CheckError(err)
	res.listener, err = net.ListenTCP("tcp", tcpAddr)
	configs.CheckError(err)
	return res
}

func (c *Commu) Run() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			select {
			case <-c.done:
				return
			default:
				configs.CheckError(err)
			}
		}
		go c.handleRequest(conn)
	}
}

func (c *Commu) Close() {
	c.done <- true
	c.connMap.Range(func(key, value interface{}) bool {
		configs.CheckError(value.(net.Conn).Close())
		return true
	})
	configs.CheckError(c.listener.Close())
}

func (c *Commu) handleRequest(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		data, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		configs.CheckError(err)
		go c.stmt.handleRequestType([]byte(data))
	}
}

func (c *Commu) connHandler(conn net.Conn) {
	ch := make(chan []byte)
	eCh := make(chan error)
	reader := bufio.NewReader(conn)
	go func(ch chan []byte, eCh chan error) {
		for {
			data, err := reader.ReadString('\n')
			if err != nil {
				eCh <- err
				return
			}
			ch <- []byte(data)
		}
	}(ch, eCh)

	ticker := time.Tick(time.Second)
	for {
		select {
		case data := <-ch:
			c.stmt.handleRequestType(data)
		case err := <-eCh:
			configs.TPrintf(err.Error())
		case <-ticker:
		}
	}
}

func (c *Commu) sendMsg(to string, msg []byte) {
	var conn net.Conn
	if cur, ok := c.connMap.Load(to); !ok {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", to)
		configs.CheckError(err)
		newConn, err := net.DialTCP("tcp", nil, tcpAddr)
		fin, _ := c.connMap.LoadOrStore(to, newConn)
		conn = fin.(net.Conn)
	} else {
		conn = cur.(net.Conn)
	}
	msg = append(msg, "\n"...)
	err := conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		configs.Warn(false, err.Error())
	}
	_, err = conn.Write(msg)
	if err != nil {
		configs.Warn(false, err.Error())
	}
}

func (stmt *Context) handleRequestType(requestBytes []byte) {
	var request network.Response4Coordinator
	err := json.Unmarshal(requestBytes, &request)
	configs.CheckError(err)
	if request.ACK {
		configs.TxnPrint(request.TID, "CA Got message with Mark "+request.Mark+" sign: true")
	} else {
		configs.TxnPrint(request.TID, "CA Got message with Mark "+request.Mark+" sign: false")
	}
	if request.Mark == configs.ReadUnsuccessful {
		stmt.Manager.handlePreRead(request.TID, &request.Read, false)
	} else if request.Mark == configs.ReadSuccess {
		stmt.Manager.handlePreRead(request.TID, &request.Read, true)
	} else if request.Mark == configs.Finished || request.Mark == configs.PreCommitACK ||
		request.Mark == configs.PreWriteACK {
		stmt.Manager.handleACK(request.TID, &request)
	} else if request.Mark == configs.FLACResults {
		stmt.Manager.handleFLAC(request.TID, &request.Res)
	}
}
