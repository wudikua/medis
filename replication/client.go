package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

type ReplicationClient struct {
	conn      net.Conn
	reader    *bufio.Reader
	stop      chan bool
	running   bool
	rdbLength int
	sync.Mutex
}

func NewReplicationClient() *ReplicationClient {
	return &ReplicationClient{
		rdbLength: 0,
		stop:      make(chan bool, 1),
		running:   false,
	}
}

func (self *ReplicationClient) Connect(host string, port int) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	self.conn = conn
	self.reader = bufio.NewReader(self.conn)
	return nil
}

func (self *ReplicationClient) SlaveRDB() error {
	self.Lock()
	self.running = true
	self.Unlock()
	self.conn.Write([]byte("*1\r\n$4\r\nSYNC\r\n"))
	line, _, err := self.reader.ReadLine()
	if err != nil {
		return err
	}
	length, err := strconv.Atoi(strings.TrimLeft(string(line), "$"))
	self.rdbLength = length
	return nil
}

func (self *ReplicationClient) GetOriginConnection() net.Conn {
	return self.conn
}

func (self *ReplicationClient) GetBufferedConnection() *bufio.Reader {
	return self.reader
}
