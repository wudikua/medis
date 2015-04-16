package mysql

import (
	"log"
	"testing"
)

func TestNew(t *testing.T) {
	client, err := NewMysqlClient("root", "root", "localhost", 8889, "test", "utf8")
	if err != nil {
		t.Fatal(err)
	}
	log.Println(client)
}
