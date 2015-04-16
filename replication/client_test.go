package replication

import (
	"medis/decoder"
	"testing"
)

func TestSlave(t *testing.T) {
	client := NewReplicationClient(&decoder.RdbDecoder{})
	err := client.Connect("localhost", 6379)
	if err != nil {
		t.Fatal(err)
	}
	client.SlaveRDB()
}
