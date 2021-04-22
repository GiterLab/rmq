package main

import (
	"github.com/GiterLab/rmq"
	"log"
)

func main() {
	log.Print("debug")
	rmq.AddDebugFunc(func(format string, level int, v ...interface{}) {
		log.Printf("AddDebugFunc"+format, v...)
	})
	rmq.TraceError("error v1 %s", "david")

}
