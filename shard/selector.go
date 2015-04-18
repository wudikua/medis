package shard

import (
	"hash/crc32"
	"medis/datasource"
	"sync"
)

// 根据key选择一个合适的group
type Selector struct {
	name    string // 可能是一个zk上的path 订阅group的扩容时的新增
	runtime []*datasource.Group
	scale   []*datasource.Group
	scaling bool
	lock    sync.RWMutex
}

func NewSelector(name string) *Selector {
	var l sync.RWMutex
	return &Selector{
		name:    name,
		lock:    l,
		scaling: false,
		runtime: make([]*datasource.Group, 0),
		scale:   make([]*datasource.Group, 0),
	}
}

func (self *Selector) AddGroup(group *datasource.Group) {
	self.runtime = append(self.runtime, group)
}

// 有可能多写，所以这里group返回的是数组，那么上层应该双写
func (self *Selector) Shard(key string, isWrite bool) []*datasource.Group {
	data := []byte(key)
	ds := make([]*datasource.Group, 1)
	if !isWrite || !self.scaling {
		i := int(crc32.ChecksumIEEE(data)) % len(self.runtime)
		ds[0] = self.runtime[i]
	} else {
		oldIdx := int(crc32.ChecksumIEEE(data)) % len(self.runtime)
		newIdx := int(crc32.ChecksumIEEE(data)) % len(self.scale)
		// 实际上这部分扩容期间的写入是冗余的
		ds[0] = self.runtime[oldIdx]
		if oldIdx != newIdx {
			ds = append(ds, self.scale[newIdx])
		}
	}
	return ds
}

func (self *Selector) All() []*datasource.Group {
	return self.runtime
}
