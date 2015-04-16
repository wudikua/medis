package adapter

import (
	"fmt"
	"medis/mysql"
	"testing"
)

func newAdapter() (*StringAdapter, error) {
	client, err := mysql.NewMysqlClient("root", "root", "localhost", 8889, "test", "utf8")
	if err != nil {
		return nil, err
	}
	adapter, err := NewStringAdapter(client)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

func TestNewString(t *testing.T) {
	adapter, err := newAdapter()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(adapter)
}

func TestSet(t *testing.T) {
	adapter, err := newAdapter()
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Set("aaa", []byte("bbb"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestGet(t *testing.T) {
	adapter, err := newAdapter()
	if err != nil {
		t.Fatal(err)
	}
	value, err := adapter.Get("aaa")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(value))
}
