package main

import (
	"fmt"
	"time"

	"github.com/GiterLab/rmq"
)

func main() {
	var msgChan chan rmq.MessageWithRoutingKey

	// 初始化 Rabbitmq 客户端
	rmq.Debug(true)
	c := rmq.NewClient()
	c.SetURL(url, vhost, username, password)
	c.SetExchange(exchange, rmq.ExchangeTopic)
	c.SetRoutingKey("giterlab")
	c.SetQueueName("giterlab-queue")
	c.Info()

	// 发布消息
	msgChan = make(chan rmq.MessageWithRoutingKey, 100)
	rmq.PublishWithRoutingKey(msgChan, c)
	for i := 0; i < 1000; i++ {
		msg := rmq.MessageWithRoutingKey{
			Message: []byte(fmt.Sprintf("giterlab-msg-%d", i)),
		}
		if i%2 == 0 {
			msg.RoutingKey = "sub1"
		} else {
			msg.RoutingKey = "sub2"
		}
		msgChan <- msg
	}

	// wait a moment
	time.Sleep(2 * time.Second)
}
