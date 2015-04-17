package datasource

import (
	"log"
	"medis/mysql"
	"testing"
)

func TestGroup(t *testing.T) {
	client, err := mysql.NewMysqlClient("root", "root", "localhost", 8889, "test", "utf8")
	if err != nil {
		t.Fatal(err)
	}
	master := NewClientWeightWrapper("group0_m0", client, 0, 1, 0, 1)
	slave0 := NewClientWeightWrapper("group0_s0", client, 1, 1, 1, 1)
	slave1 := NewClientWeightWrapper("group0_s1", client, 2, 0, 1, 0)
	group0 := NewGroup()
	group0.AddClient(master)
	group0.AddClient(slave0)
	group0.AddClient(slave1)
	group0.Init()

	log.Println("fetch group0 write db")
	for i := 0; i < 10; i++ {
		log.Println(group0.GetDB(true).name)
	}
	log.Println("fetch group0 read db")
	for i := 0; i < 10; i++ {
		log.Println(group0.GetDB(false).name)
	}
}
