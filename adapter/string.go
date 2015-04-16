package adapter

import (
	"fmt"
	"medis/mysql"
)

type StringAdapter struct {
	db *DBAdapter
}

func NewStringAdapter(client *mysql.MysqlClient) (*StringAdapter, error) {
	db, err := NewDBAdapter(client)
	if err != nil {
		return nil, err
	}
	return &StringAdapter{
		db: db,
	}, nil
}

func (self *StringAdapter) String() string {
	return fmt.Sprintf("StringAdapter|%s", self.db)
}

func (self *StringAdapter) Set(key string, value []byte) error {
	id, err := self.db.GenKey(key, KEY_TYPE_STRING)
	if err != nil {
		return err
	}
	db := self.db.client.GetDB()
	stmt, err := db.Prepare("INSERT INTO `test`.`string` (`id`, `value`) VALUES (?, ?)")
	defer stmt.Close()
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id, value)
	if err != nil {
		return err
	}
	return nil
}

func (self *StringAdapter) Get(key string) ([]byte, error) {
	db := self.db.client.GetDB()
	var value []byte
	err := db.QueryRow("SELECT `string`.`value` FROM `string` left join `db` on `db`.`id`=`string`.`id` WHERE `db`.`key`=?", key).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (self *StringAdapter) Del(id int) error {
	db := self.db.client.GetDB()
	stmt, err := db.Prepare("delete from `test`.`string` where `test`.`string`.`id`=?")
	defer stmt.Close()
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id)
	return err
}

func (self *StringAdapter) FlushAll() error {
	return self.db.FlushTable("string")
}
