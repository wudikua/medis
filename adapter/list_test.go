package adapter

import (
	"fmt"
	"medis/mysql"
	"testing"
)

func newListAdapter() (*ListAdapter, error) {
	client, err := mysql.NewMysqlClient("root", "root", "localhost", 8889, "test", "utf8")
	if err != nil {
		return nil, err
	}
	adapter, err := NewListAdapter(client)
	if err != nil {
		return nil, err
	}
	return adapter, nil
}

func TestRPUSH(t *testing.T) {
	adapter, err := newListAdapter()
	if err != nil {
		t.Fatal(err)
	}
	length, err := adapter.Rpush("list1", []byte("l1"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("push length ", length, " \n")
}

func TestLRANGE(t *testing.T) {
	adapter, err := newListAdapter()
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Lrange("list1", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range result {
		fmt.Println("push length[", i, "]=", string(v))
	}
}
