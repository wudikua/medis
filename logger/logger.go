package logger

import (
	"log"
)

func LogInfo(arg ...interface{}) {
	log.Println(arg)
}
