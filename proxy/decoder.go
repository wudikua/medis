package proxy

import (
	"log"
	"medis/rdb"
)

type RdbDecoder struct {
	rdb.NopDecoder
	finish  chan bool
	handler *MedisHandler
}

func NewRdbDecoder(handler *MedisHandler) (dec *RdbDecoder) {
	return &RdbDecoder{
		finish:  make(chan bool, 1),
		handler: handler,
	}
}

func (self *RdbDecoder) Set(key, value []byte, expiry int64) {
	self.handler.Set(string(key), value)
}

func (self *RdbDecoder) Hset(key, field, value []byte) {
	self.handler.Hset(string(key), string(field), value)
}

func (self *RdbDecoder) Rpush(key, value []byte) {
	self.handler.Rpush(string(key), value)
}

func (self *RdbDecoder) Zadd(key []byte, score float64, member []byte) {
	self.handler.Zadd(string(key), int(score), member)
}

func (self *RdbDecoder) Sadd(key, member []byte) {

}

func (self *RdbDecoder) EndRDB() {
	log.Println("sync rdb finish")
	self.finish <- true
}

func (self *RdbDecoder) StartRDB() {

}

func (self *RdbDecoder) StartDatabase(n int) {

}

func (self *RdbDecoder) EndHash(key []byte) {

}

func (self *RdbDecoder) StartSet(key []byte, cardinality, expiry int64) {

}

func (self *RdbDecoder) EndSet(key []byte) {

}

func (self *RdbDecoder) StartList(key []byte, length, expiry int64) {

}

func (self *RdbDecoder) EndList(key []byte) {

}

func (self *RdbDecoder) StartZSet(key []byte, cardinality, expiry int64) {

}

func (self *RdbDecoder) EndZSet(key []byte) {

}

func (self *RdbDecoder) EndDatabase(n int) {

}
