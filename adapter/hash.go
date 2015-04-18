package adapter

import (
	"fmt"
)

type HashAdapter struct {
	db *DBAdapter
}

func NewHashAdapter(db *DBAdapter) (*HashAdapter, error) {
	return &HashAdapter{
		db: db,
	}, nil
}

func (self *HashAdapter) String() string {
	return fmt.Sprintf("HashAdapter|%s", self.db)
}

func (self *HashAdapter) HSet(key, hkey string, value []byte) error {
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		id, err := self.db.GenKey(key, KEY_TYPE_HASH, g.GetDB(true).GetClient())
		if err != nil {
			return err
		}
		stmt, err := db.Prepare("INSERT INTO `test`.`hash` (`id`, `hkey`, `hvalue`) VALUES (?, ?, ?)")
		defer stmt.Close()
		if err != nil {
			return err
		}
		_, err = stmt.Exec(id, hkey, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *HashAdapter) HGet(key, hkey string) ([]byte, error) {
	db := self.db.GetReaderClient(key).GetDB()
	var value []byte
	err := db.QueryRow("SELECT `hash`.`hvalue` FROM `hash` left join `db` on `db`.`id`=`hash`.`id` WHERE `hash`.`hkey`=? and `db`.`key`=? ", hkey, key).Scan(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (self *HashAdapter) HGetall(key string) ([][]byte, error) {
	db := self.db.GetReaderClient(key).GetDB()
	result := make([][]byte, 0)
	var hkey, hvalue string
	rows, err := db.Query("SELECT `hash`.`hkey`,`hash`.`hvalue` FROM `hash` left join `db` on `db`.`id`=`hash`.`id` WHERE `db`.`key`=? ", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&hkey, &hvalue)
		if err != nil {
			return nil, err
		}
		result = append(result, []byte(hkey))
		result = append(result, []byte(hvalue))
	}
	return result, nil
}

func (self *HashAdapter) Del(key string) error {
	id := self.db.GetKeyID(key)
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("delete from `test`.`hash` where `test`.`hash`.`id`=?")
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

func (self *HashAdapter) FlushAll() error {
	return self.db.FlushTable("hash")
}
