package main

import (
	"medis/proxy"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		http.ListenAndServe(":13800", nil)
	}()
	proxy.ListenAndServeRedis()
}
