package adapter

import (
	"fmt"
	"medis/mysql"
	"testing"
)

func newHashAdapter() (*HashAdapter, error) {
	client, err := mysql.NewMysqlClient("root", "root", "localhost", 8889, "test", "utf8")
	if err != nil {
		return nil, err
	}
	adapter, err := NewHashAdapter(client)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

func TestHSet(t *testing.T) {
	adapter, err := newHashAdapter()
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.HSet("h1", "hkey1", []byte("hvalue1"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestHGet(t *testing.T) {
	adapter, err := newHashAdapter()
	if err != nil {
		t.Fatal(err)
	}
	value, err := adapter.HGet("h1", "hkey1")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(value))
}
