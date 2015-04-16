package proxy

import (
	"flag"
	redis "github.com/wudikua/go-redis-server"
	"log"
)

var server *redis.Server

func ListenAndServeRedis() {
	var host string
	var port int
	flag.StringVar(&host, "h", "localhost", "host")
	flag.IntVar(&port, "p", 6389, "port")
	flag.Parse()

	// 启动redis server
	handler, err := NewMedisHandler()
	if err != nil {
		log.Fatal("server crash on start ", err)
	}
	server, _ = redis.NewServer(redis.DefaultConfig().Proto("tcp").Host(host).Port(port).Handler(handler))

	server.ListenAndServe()
}
