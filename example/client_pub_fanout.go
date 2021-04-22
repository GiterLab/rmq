package main

import (
	"fmt"
	"log"
	"time"

	"github.com/GiterLab/rmq"
)

func main() {
	const (
		url      = "127.0.0.1:5672"
		vhost    = "/"
		username = "guest"
		password = "guest"
		exchange = "giterlab-pub-fanout"
	)

	var msgChan chan rmq.Message

	// 初始化 Rabbitmq 客户端
	rmq.Debug(true)
	rmq.AddDebugFunc(func(format string, level int, v ...interface{}) {
		log.Print(format, v)
	})
	c := rmq.NewClient()
	c.SetURL(url, vhost, username, password)
	c.SetExchange(exchange, rmq.ExchangeFanout)
	c.SetRoutingKey("giterlab")
	c.SetQueueName("giterlab-queue")
	c.Info()

	// 发布消息
	msgChan = make(chan rmq.Message, 100)
	rmq.Publish(msgChan, c)
	for i := 0; i < 1000; i++ {
		msgChan <- []byte(fmt.Sprintf("giterlab-msg-%d", i))
	}

	// wait a moment
	time.Sleep(2 * time.Second)
}
