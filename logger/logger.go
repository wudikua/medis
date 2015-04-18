package logger

import (
	"log"
	"runtime"
)

func LogInfo(arg ...interface{}) {
	log.Println("INFO|", arg)
}

func LogDebug(arg ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	log.Println("DEBUG|", arg, "in", file, "line:", line)
}
