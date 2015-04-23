package proxy

import (
	"fmt"
	"log"
	"medis/adapter"
	"medis/logger"
	"medis/rdb"
	"medis/replication"
	"sync"
)

type MedisHandler struct {
	dbAdapter     *adapter.DBAdapter
	stringAdapter *adapter.StringAdapter
	hashAdapter   *adapter.HashAdapter
	listAdapter   *adapter.ListAdapter
	zsetAdapter   *adapter.ZSetAdapter
	lock          sync.RWMutex
}

func NewMedisHandler(dbAdapter *adapter.DBAdapter) (*MedisHandler, error) {
	var lock sync.RWMutex
	var err error
	medisHandler := &MedisHandler{
		lock: lock,
	}
	medisHandler.dbAdapter = dbAdapter
	medisHandler.stringAdapter, err = adapter.NewStringAdapter(medisHandler.dbAdapter)
	if err != nil {
		return nil, err
	}
	medisHandler.hashAdapter, err = adapter.NewHashAdapter(medisHandler.dbAdapter)
	if err != nil {
		return nil, err
	}
	medisHandler.listAdapter, err = adapter.NewListAdapter(medisHandler.dbAdapter)
	if err != nil {
		return nil, err
	}
	medisHandler.zsetAdapter, err = adapter.NewZSetAdapter(medisHandler.dbAdapter)
	if err != nil {
		return nil, err
	}
	return medisHandler, nil
}

func (self *MedisHandler) Ping() ([]byte, error) {
	log.Println("PING FROM CLIENT")
	return []byte("PONG"), nil
}

func (self *MedisHandler) Slaveof(host string, port int) error {
	client := replication.NewReplicationClient()
	err := client.Connect(host, port)
	if err != nil {
		return err
	}
	err = client.SlaveRDB()
	if err != nil {
		return err
	}
	decoder := NewRdbDecoder(self)
	rdb.DecodeFromBufio(client.GetBufferedConnection(), decoder)
	<-decoder.finish
	go func() {
		conn := client.GetOriginConnection()
		server.ServeReplClient(conn)
	}()
	return nil
}

func (self *MedisHandler) Type(key string) ([]byte, error) {
	keyType, err := self.dbAdapter.Type(key)
	if err != nil {
		return nil, err
	}
	return []byte(keyType), nil
}

func (self *MedisHandler) Del(key string) (int, error) {
	keyType := self.dbAdapter.GetKeyType(key)
	switch keyType {
	case adapter.KEY_TYPE_STRING:
		self.stringAdapter.Del(key)
	case adapter.KEY_TYPE_HASH:
		self.hashAdapter.Del(key)
	case adapter.KEY_TYPE_LIST:
		self.listAdapter.Del(key)
	case adapter.KEY_TYPE_ZSET:
		self.zsetAdapter.Del(key)
	}
	return 1, self.dbAdapter.Del(key)
}

func (self *MedisHandler) FlushAll() error {
	self.dbAdapter.FlushAll()
	self.stringAdapter.FlushAll()
	self.listAdapter.FlushAll()
	self.hashAdapter.FlushAll()
	self.zsetAdapter.FlushAll()
	return nil
}

func (self *MedisHandler) Get(key string) ([]byte, error) {
	return self.stringAdapter.Get(key)
}

func (self *MedisHandler) Mget(keys []string) ([][]byte, error) {
	logger.LogDebug("mget", keys)
	return self.stringAdapter.MGet(keys)
}

func (self *MedisHandler) Set(key string, value []byte) error {
	return self.stringAdapter.Set(key, value)
}

func (self *MedisHandler) Hget(key, subkey string) ([]byte, error) {
	return self.hashAdapter.HGet(key, subkey)
}

func (self *MedisHandler) Hgetall(key string) ([][]byte, error) {
	return self.hashAdapter.HGetall(key)
}

func (self *MedisHandler) Hset(key, subkey string, value []byte) (int, error) {
	err := self.hashAdapter.HSet(key, subkey, value)
	if err != nil {
		return 0, err
	}
	return 1, err
}

func (self *MedisHandler) Rpush(key string, value []byte, values ...[]byte) (int, error) {
	ret, err := self.listAdapter.Rpush(key, value, values...)
	if err != nil {
		log.Println(err)
	}
	return ret, err
}

func (self *MedisHandler) Lrange(key string, start, stop int) ([][]byte, error) {
	ret, err := self.listAdapter.Lrange(key, start, stop)
	if err != nil {
		log.Println(err)
	}
	return ret, err
}

func (self *MedisHandler) Zadd(key string, score int, value []byte) (int, error) {
	return 1, self.zsetAdapter.Zadd(key, score, value)
}

func (self *MedisHandler) Zcard(key string) (int, error) {
	return self.zsetAdapter.Zcard(key)
}

func (self *MedisHandler) Zrange(key string, start, end int, WITHSCORES ...[]byte) ([][]byte, error) {
	var withscore bool
	if len(WITHSCORES) > 0 {
		withscore = true
	} else {
		withscore = false
	}
	result, err := self.zsetAdapter.Zrange(key, start, end, withscore)
	return result, err
}

func (self *MedisHandler) Zrangebyscore(key string, min, max int, WITHSCORES ...[]byte) ([][]byte, error) {
	var withscore bool
	if len(WITHSCORES) > 0 {
		withscore = true
	} else {
		withscore = false
	}
	result, err := self.zsetAdapter.Zrangebyscore(key, min, max, withscore)
	return result, err
}

func (self *MedisHandler) Zrem(key string, value []byte) (int, error) {
	err := self.zsetAdapter.Zrem(key, value)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (self *MedisHandler) Zscore(key string, value []byte) ([]byte, error) {
	score, err := self.zsetAdapter.Zscore(key, value)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("%d", score)), nil
}
