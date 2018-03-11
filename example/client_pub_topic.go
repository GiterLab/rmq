package main

import (
	"fmt"
	"time"

	"github.com/GiterLab/rmq"
	"github.com/astaxie/beego/logs"
)

// GLog 全局日志变量
var GLog *logs.BeeLogger

const (
	url      = "127.0.0.1:5672"
	vhost    = "/"
	username = "guest"
	password = "guest"
	exchange = "giterlab-pub-topic"
)

func main() {
	var msgChan chan rmq.MessageWithRoutingKey

	// 设置日志
	GLog = logs.NewLogger(10000)
	GLog.SetLogger("console", `{"level":7}`)
	GLog.EnableFuncCallDepth(true)
	GLog.SetLogFuncCallDepth(3)

	// 初始化 Rabbitmq 客户端
	rmq.Debug(true)
	rmq.SetLogger(GLog)
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
