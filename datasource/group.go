package datasource

import (
	"fmt"
	"sync"
)

// 一组数据源，维护了读写分离，主备切换
type Group struct {
	name    string
	reader  *ClientPriorityList
	writer  *ClientPriorityList
	clients []*ClientWeightWrapper
	reload  sync.RWMutex
}

type ClientWeightSame []*ClientWeightWrapper

func NewGroup(name string) *Group {
	var mutex sync.RWMutex
	return &Group{
		name:    name,
		clients: make([]*ClientWeightWrapper, 0),
		reload:  mutex,
	}
}

func (self *Group) String() string {
	return fmt.Sprintf("name:%s clients:%s\n", self.name, self.clients)
}

func (self *Group) AddClient(client *ClientWeightWrapper) {
	self.clients = append(self.clients, client)
}

func (self *Group) Init() {
	self.reload.Lock()
	defer self.reload.Unlock()
	self.reader = self.parseProiority(false)
	self.writer = self.parseProiority(true)
}

func (self *Group) GetDB(isWrite bool) *ClientWeightWrapper {
	if isWrite {
		return self.writer.SelectWrite()
	} else {
		return self.reader.SelectRead()
	}
}

func (self *Group) parseProiority(isWrite bool) *ClientPriorityList {
	// key是优先级 value是同优先级下不同权重的一组数据源
	clientLevel := make(map[int]ClientWeightSame)
	var compareP int
	for _, c := range self.clients {
		if isWrite {
			compareP = c.q
		} else {
			compareP = c.p
		}
		if compareP > 0 {
			if clientLevel[compareP] == nil {
				clientLevel[compareP] = make(ClientWeightSame, 0)
			}
			clientLevel[compareP] = append(clientLevel[compareP], c)
		}
	}
	// 头指针
	var head *ClientPriorityList
	// 工作指针
	var pWork *ClientPriorityList
	for p, cs := range clientLevel {
		if head == nil {
			head = NewClientPriorityList(cs, p)
			continue
		}
		// p的优先级比头指针还大，做头插法
		if head.priority < p {
			pWork = head
			head = NewClientPriorityList(cs, p)
			head.lowerPriority = pWork
		}
	}
	return head
}
