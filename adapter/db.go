package adapter

import (
	"fmt"
	"medis/logger"
	"medis/mysql"
	"medis/shard"
)

const (
	KEY_TYPE_STRING = 0
	KEY_TYPE_HASH   = 1
	KEY_TYPE_LIST   = 2
	KEY_TYPE_ZSET   = 3
)

type DBAdapter struct {
	selector *shard.Selector
}

func NewDBAdapter(selector *shard.Selector) (*DBAdapter, error) {
	return &DBAdapter{
		selector: selector,
	}, nil
}

func (self *DBAdapter) GetReaderClient(key string) *mysql.MysqlClient {
	return self.selector.Shard(key, false)[0].GetDB(false).GetClient()
}

func (self *DBAdapter) String() string {
	return fmt.Sprintf("DBAdapter")
}

func (self *DBAdapter) GenKey(key string, keyType int, client *mysql.MysqlClient) (int64, error) {
	logger.LogDebug("gen key use client", client)
	db := client.GetDB()
	id := int64(-1)
	db.QueryRow("SELECT `db`.`id` from `db` WHERE `db`.`key`=?", key).Scan(&id)
	if id < 0 {
		stmt, err := db.Prepare("INSERT INTO `db` (`id`, `type`, `key`) VALUES (NULL, ?, ?)")
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

func (self *DBAdapter) GetKeyType(key string) int {
	groups := self.selector.Shard(key, false)
	db := groups[0].GetDB(false).GetClient().GetDB()
	keyType := -1
	db.QueryRow("SELECT `db`.`type` FROM `db` WHERE `db`.`key`=?", key).Scan(&keyType)
	return keyType
}

func (self *DBAdapter) GetKeyID(key string) int {
	groups := self.selector.Shard(key, false)
	db := groups[0].GetDB(false).GetClient().GetDB()
	keyID := -1
	db.QueryRow("SELECT `db`.`id` FROM `db` WHERE `db`.`key`=?", key).Scan(&keyID)
	return keyID
}

func (self *DBAdapter) Type(key string) (string, error) {
	keyType := self.GetKeyType(key)
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
	groups := self.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("DELETE FROM `db` where `db`.`key`=?")
		defer stmt.Close()
		if err != nil {
			return err
		}
		_, err = stmt.Exec(key)
		if err != nil {
			return err
		}
	}
	return nil

}

func (self *DBAdapter) FlushTable(table string) error {
	groups := self.selector.All()
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("delete from `" + table + "`")
		defer stmt.Close()
		if err != nil {
			return err
		}
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *DBAdapter) FlushAll() error {
	return self.FlushTable("db")
}
