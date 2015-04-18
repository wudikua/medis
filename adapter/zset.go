package adapter

import (
	"errors"
	"fmt"
)

type ZSetAdapter struct {
	db *DBAdapter
}

func NewZSetAdapter(db *DBAdapter) (*ZSetAdapter, error) {
	return &ZSetAdapter{
		db: db,
	}, nil
}

func (self *ZSetAdapter) String() string {
	return fmt.Sprintf("ZSetAdapter|%s", self.db)
}

func (self *ZSetAdapter) Zadd(key string, score int, value []byte) error {
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		id, err := self.db.GenKey(key, KEY_TYPE_ZSET, g.GetDB(true).GetClient())
		if err != nil {
			return err
		}
		stmt, err := db.Prepare("INSERT INTO `zset` (`id`, `score`, `value`) VALUES (?, ?, ?)")
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.Exec(id, score, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *ZSetAdapter) Zcard(key string) (int, error) {
	db := self.db.GetReaderClient(key).GetDB()
	count := 0
	err := db.QueryRow("SELECT count(*) FROM `zset` left join `db` on `db`.`id`=`zset`.`id` WHERE `db`.`key`=? ", key).Scan(&count)
	if err != nil {
		return count, err
	}
	return count, nil
}

func (self *ZSetAdapter) Zrange(key string, start, stop int, WITHSCORES bool) ([][]byte, error) {
	size := stop - start
	if size < 0 {
		return nil, errors.New("stop - start less than 0")
	}
	var result [][]byte
	if WITHSCORES {
		result = make([][]byte, size*2)
	} else {
		result = make([][]byte, size)
	}
	value := ""
	score := 0
	db := self.db.GetReaderClient(key).GetDB()
	rows, err := db.Query("select `zset`.`score`,`zset`.`value` from `zset` left join `db` on `db`.`id`=`zset`.`id` WHERE `db`.`key`=? order by score limit ?, ?", key, start, size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		err := rows.Scan(&score, &value)
		if err != nil {
			return nil, err
		}
		result[i] = []byte(value)
		i += 1
		if WITHSCORES {
			result[i] = []byte(fmt.Sprintf("%d", score))
			i += 1
		}
	}
	return result, nil
}

func (self *ZSetAdapter) Zrangebyscore(key string, min, max int, WITHSCORES bool) ([][]byte, error) {
	result := make([][]byte, 0)
	value := ""
	score := 0
	db := self.db.GetReaderClient(key).GetDB()
	rows, err := db.Query("select `zset`.`score`,`zset`.`value` from `zset` left join `db` on `db`.`id`=`zset`.`id` WHERE `db`.`key`=? and `zset`.`score` between ? and ?", key, min, max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&score, &value)
		if err != nil {
			return nil, err
		}
		result = append(result, []byte(value))
		if WITHSCORES {
			result = append(result, []byte(fmt.Sprintf("%d", score)))
		}
	}
	return result, nil
}

func (self *ZSetAdapter) Zrem(key string, value []byte) error {
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		// 迁移状态时候可能获取不到id
		id := self.db.GetKeyID(key)
		stmt, err := db.Prepare("delete from `zset` where `zset`.`id`=? and `zset`.`value`=?")
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.Exec(id, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *ZSetAdapter) Zscore(key string, value []byte) (int, error) {
	db := self.db.GetReaderClient(key).GetDB()
	var score int
	err := db.QueryRow("select `zset`.`score` from `zset` left join `db` on `db`.`id`=`zset`.`id` WHERE `db`.`key`=? and value =? ", key, value).Scan(&score)
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (self *ZSetAdapter) Del(key string) error {
	id := self.db.GetKeyID(key)
	groups := self.db.selector.Shard(key, true)
	for _, g := range groups {
		db := g.GetDB(true).GetClient().GetDB()
		stmt, err := db.Prepare("delete from `zset` where `zset`.`id`=?")
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

func (self *ZSetAdapter) FlushAll() error {
	return self.db.FlushTable("zset")
}
