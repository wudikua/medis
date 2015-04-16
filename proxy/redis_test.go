package proxy

import (
	"testing"
)

func TestRedis(t *testing.T) {
	ListenAndServeRedis()
	t.Log("server shutdown")
}
