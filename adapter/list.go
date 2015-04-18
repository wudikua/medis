package adapter

import (
	"errors"
	"fmt"
	"sync"
)

type ListAdapter struct {
	db *DBAdapter
	sync.RWMutex
}

func NewListAdapter(db *DBAdapter) (*ListAdapter, error) {
	return &ListAdapter{
		db: db,
	}, nil
}

func (self *ListAdapter) String() string {
	return fmt.Sprintf("ListAdapter|%s", self.db)
}

func (self *ListAdapter) Rpush(key string, value []byte, values ...[]byte) (int, error) {
	groups := self.db.selector.Shard(key, true)
	maxLength := 0
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		id, err := self.db.GenKey(key, KEY_TYPE_LIST, g.GetDB(true).GetClient())
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		self.Lock()
		length := 0
		defer self.Unlock()
		_ = db.QueryRow("SELECT `list`.`index` FROM `list` left join `db` on `db`.`id`=`list`.`id` WHERE `db`.`key`=? order by `list`.`index` desc limit 1", key).Scan(&length)
		stmt, err := db.Prepare("INSERT INTO `list` (`id`, `index`, `value`) VALUES (?, ?, ?)")
		defer stmt.Close()
		if err != nil {
			return 0, err
		}
		length += 1
		_, err = stmt.Exec(id, length, value)
		if err != nil {
			return 0, err
		}
		for _, val := range values {
			length += 1
			_, err = stmt.Exec(id, length, val)
			if err != nil {
				return 0, err
			}
		}
		if length > maxLength {
			maxLength = length
		}
	}
	return maxLength, nil
}

func (self *ListAdapter) Lrange(key string, start, stop int) ([][]byte, error) {
	size := stop - start + 1
	if size < 0 {
		return nil, errors.New("stop - start less than 0")
	}
	result := make([][]byte, size)
	db := self.db.GetReaderClient(key).GetDB()
	var value []byte
	rows, err := db.Query("SELECT `list`.`value` FROM `list` left join `db` on `db`.`id`=`list`.`id` WHERE `db`.`key`=? and `list`.`index` between ? and ?",
		key, start, stop+1)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	i := 0
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		if i >= size {
			return nil, errors.New(fmt.Sprintf("index out of range %d %d", i, stop-start))
		}
		result[i] = value
		i += 1
	}
	return result, nil
}

func (self *ListAdapter) Del(key string) error {
	id := self.db.GetKeyID(key)
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("delete from `list` where `list`.`id`=?")
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

func (self *ListAdapter) FlushAll() error {
	return self.db.FlushTable("list")
}
