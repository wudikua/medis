package adapter

import (
	"fmt"
)

type StringAdapter struct {
	db *DBAdapter
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
