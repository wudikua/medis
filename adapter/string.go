package adapter

import (
	"fmt"
	"sync"
)

type StringAdapter struct {
	db *DBAdapter
}

type Pair struct {
	idx  int
	data []byte
}

func NewStringAdapter(db *DBAdapter) (*StringAdapter, error) {
	return &StringAdapter{
		db: db,
	}, nil
}

func (self *StringAdapter) String() string {
	return fmt.Sprintf("StringAdapter|%s", self.db)
}

func (self *StringAdapter) Set(key string, value []byte) error {
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		id, err := self.db.GenKey(key, KEY_TYPE_STRING, g.GetDB(true).GetClient())
		if err != nil {
			return err
		}
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("INSERT INTO `string` (`id`, `value`) VALUES (?, ?)")
		defer stmt.Close()
		if err != nil {
			return err
		}
		_, err = stmt.Exec(id, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *StringAdapter) Get(key string) ([]byte, error) {
	db := self.db.GetReaderClient(key).GetDB()
	var value []byte
	err := db.QueryRow("SELECT `string`.`value` FROM `string` left join `db` on `db`.`id`=`string`.`id` WHERE `db`.`key`=?", key).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (self *StringAdapter) MGet(keys []string) ([][]byte, error) {
	result := make([][]byte, len(keys))
	dataC := make(chan Pair, len(keys))
	errC := make(chan error, len(keys))
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func() {
			qkey := key
			qi := i
			wg.Done()
			data, err := self.Get(qkey)
			if err != nil {
				errC <- err
			}
			dataC <- Pair{qi, data}
		}()
		wg.Wait()
	}
	finish := 0
	for finish < len(keys) {
		select {
		case pair := <-dataC:
			result[pair.idx] = pair.data
		case err := <-errC:
			return nil, err
		}
		finish += 1
	}
	return result, nil
}

func (self *StringAdapter) Del(key string) error {
	id := self.db.GetKeyID(key)
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("delete from `string` where `string`.`id`=?")
		defer stmt.Close()
		if err != nil {
			return err
		}
		_, err = stmt.Exec(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *StringAdapter) FlushAll() error {
	return self.db.FlushTable("string")
}
