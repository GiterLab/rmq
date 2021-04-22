package main

import (
	"github.com/GiterLab/rmq"
)

func main() {

	// 初始化 Rabbitmq 客户端
	rmq.Debug(true)
	c := rmq.NewClient()
	c.SetURL(url, vhost, username, password)
	c.SetExchange(exchange, rmq.ExchangeTopic)
	c.SetRoutingKey("sub2")
	c.SetQueueName("giterlab-queue-sub2")
	c.SetQos(10)
	c.Info()

	// 消息订阅
	go func() {
		rmq.Subscribe(func(msg []byte) {
			defer func() {
				// recover panic
				if err := recover(); err != nil {
					rmq.TraceError("[sub2] panic, %s", err)
				}
			}()

			rmq.TraceError("[sub2] %s", string(msg))
		}, c)
	}()

	// wait a moment
	select {}
}
