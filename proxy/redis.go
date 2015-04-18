package proxy

import (
	"flag"
	redis "github.com/wudikua/go-redis-server"
	"log"
)

var server *redis.Server

func ListenAndServeRedis(handler *MedisHandler) {
	var host string
	var port int
	flag.StringVar(&host, "h", "localhost", "host")
	flag.IntVar(&port, "p", 6389, "port")
	flag.Parse()

	// 启动redis server
	server, err := redis.NewServer(redis.DefaultConfig().Proto("tcp").Host(host).Port(port).Handler(handler))
	if err != nil {
		log.Fatal(err)
	}

	server.ListenAndServe()
}
