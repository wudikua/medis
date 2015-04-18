package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"medis/definition"
)

type MysqlClientContext struct {
	user     string
	password string
	host     string
	port     int
	db       string
	charset  string
}

func (self *MysqlClientContext) GetConnString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", self.user, self.password, self.host, self.port, self.db, self.charset)
}

type MysqlClient struct {
	db  *sql.DB
	ctx *MysqlClientContext
}

func NewMysqlClient(user string, password string, host string, port int, db string, charset string) (*MysqlClient, error) {
	ctx := &MysqlClientContext{
		user:     user,
		password: password,
		host:     host,
		port:     port,
		db:       db,
		charset:  charset,
	}
	conn, err := sql.Open("mysql", ctx.GetConnString())
	conn.SetMaxIdleConns(definition.MYSQL_CONN_MIN)
	conn.SetMaxOpenConns(definition.MYSQL_CONN_MAX)
	if err != nil {
		return nil, err
	}
	return &MysqlClient{
		db:  conn,
		ctx: ctx,
	}, nil
}

func (self *MysqlClient) String() string {
	return fmt.Sprintf("MysqlClient connected %s:%d %s", self.ctx.host, self.ctx.port, self.ctx.db)
}

func (self *MysqlClient) GetDB() *sql.DB {
	return self.db
}

func (self *MysqlClient) GetName() string {
	return self.ctx.db
}
