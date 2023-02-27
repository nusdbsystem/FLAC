package participant

import (
	"FLAC/configs"
	"FLAC/network"
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strconv"
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

func (c *Commu) Stop() {
	c.done <- true
	c.connMap.Range(func(key, value interface{}) bool {
		configs.CheckError(value.(net.Conn).Close())
		return true
	})
	configs.CheckError(c.listener.Close())
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
	/* Checks the kind of request sent to coordinator. Calls relevant functions based
	on the request. */
	if stmt.Manager.isBroken() {
		return
	}
	if stmt.Manager.isNF() { // simulate the network failure.
		time.Sleep(configs.NFTime)
	}
	// we receive the messages for crashed node because currently we do not have a recovery protocol.
	// In FLAC, these messages will get logged on the coordinator and sent to the participants when they recover.
	var request network.ParticipantGossip
	err := json.Unmarshal(requestBytes, &request)
	configs.CheckError(err)
	configs.DPrintf("TXN" + strconv.FormatUint(request.Txn.TxnID, 10) + ": " + "Pending message for " + stmt.address + " with Mark " + request.Mark)
	txn := request.Txn
	if request.Mark == configs.PreRead {
		/* A new Txn started involving this replica. For all protocols */
		ok, res := stmt.Manager.PreRead(&txn)
		if !ok {
			stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.ReadUnsuccessful, res)
		} else {
			stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.ReadSuccess, res)
		}
	} else if request.Mark == configs.PreWrite {
		// For 2PC, 3PC, single shard, EasyCommit, C-PAC.
		res := stmt.Manager.PreWrite(&txn)
		stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.PreWriteACK, res)
	} else if request.Mark == configs.Commit {
		// For all protocols
		configs.Assert(stmt.Manager.Commit(&txn), "The commit is not executed")
		stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.Finished, true)
	} else if request.Mark == configs.Abort {
		// For all protocols
		configs.Assert(stmt.Manager.Abort(&txn), "The abort is not executed")
		stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.Finished, false)
	} else if request.Mark == configs.FLACProposed {
		// For FLAC only.
		res := stmt.Manager.Propose(&txn, request.SentTime)
		configs.Assert(res != nil, "Nil ptr encountered for result")
		stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.FLACResults, *res)
	} else if request.Mark == configs.PreCommit {
		// For 3PC only.
		res := stmt.Manager.Agree(&txn, true)
		stmt.Manager.sendBackCA(txn.TxnID, txn.ShardID, configs.PreCommitACK, res)
	} else if request.Mark == configs.FLACGossipVotes {
		// For FLAC, EasyCommit.
		stmt.Manager.handleVote(&request.Vt)
	}
}
