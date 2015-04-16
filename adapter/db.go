package adapter

import (
	"fmt"
	"medis/mysql"
)

const (
	KEY_TYPE_STRING = 0
	KEY_TYPE_HASH   = 1
	KEY_TYPE_LIST   = 2
	KEY_TYPE_ZSET   = 3
)

type DBAdapter struct {
	client *mysql.MysqlClient
}

func NewDBAdapter(client *mysql.MysqlClient) (*DBAdapter, error) {
	return &DBAdapter{
		client: client,
	}, nil
}

func (self *DBAdapter) String() string {
	return fmt.Sprintf("DBAdapter|%s", self.client)
}

func (self *DBAdapter) GenKey(key string, keyType int) (int64, error) {
	db := self.client.GetDB()
	id := int64(-1)
	db.QueryRow("SELECT `test`.`db`.`id` from `test`.`db` WHERE `test`.`db`.`key`=?", key).Scan(&id)
	if id < 0 {
		stmt, err := db.Prepare("INSERT INTO `test`.`db` (`id`, `type`, `key`) VALUES (NULL, ?, ?)")
		defer stmt.Close()
		if err != nil {
			return -1, err
		}
		result, err := stmt.Exec(keyType, key)
		if err != nil {
			return -1, err
		}
		id, err = result.LastInsertId()
		if err != nil {
			return -1, err
		}
	}
	return id, nil
}

func (self *DBAdapter) GetKeyType(key string) (int, int) {
	db := self.client.GetDB()
	keyType := -1
	innerId := -1
	db.QueryRow("SELECT `test`.`db`.`type`,`test`.`db`.`id` FROM `test`.`db` WHERE `db`.`key`=?", key).Scan(&keyType, &innerId)
	return innerId, keyType
}

func (self *DBAdapter) Type(key string) (string, error) {
	_, keyType := self.GetKeyType(key)
	keyString := ""
	switch keyType {
	case KEY_TYPE_STRING:
		keyString = "string"
	case KEY_TYPE_HASH:
		keyString = "hash"
	case KEY_TYPE_LIST:
		keyString = "list"
	case KEY_TYPE_ZSET:
		keyString = "zset"
	default:
		keyString = "none"
	}
	return keyString, nil
}

func (self *DBAdapter) Del(key string) error {
	db := self.client.GetDB()
	stmt, err := db.Prepare("DELETE FROM `test`.`db` where `db`.`key`=?")
	defer stmt.Close()
	if err != nil {
		return err
	}
	_, err = stmt.Exec(key)
	if err != nil {
		return err
	}
	return nil
}

func (self *DBAdapter) FlushTable(table string) error {
	db := self.client.GetDB()
	stmt, err := db.Prepare("delete from `test`.`" + table + "`")
	defer stmt.Close()
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (self *DBAdapter) FlushAll() error {
	return self.FlushTable("db")
}
