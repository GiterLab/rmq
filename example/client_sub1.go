package main

import (
	"github.com/GiterLab/rmq"
	"github.com/beego/beego/v2/core/logs"
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
	// 设置日志
	GLog = logs.NewLogger(10000)
	GLog.SetLogger("console", `{"level":7}`)
	GLog.EnableFuncCallDepth(true)
	GLog.SetLogFuncCallDepth(3)

	// 初始化 Rabbitmq 客户端
	rmq.Debug(true)
	rmq.SetUserDebug(func(format string, level int, v ...interface{}) {
		switch level {
		case rmq.LevelInformational:
			GLog.Info(format, v...)
		case rmq.LevelError:
			GLog.Error(format, v...)
		}
	})
	c := rmq.NewClient()
	c.SetURL(url, vhost, username, password)
	c.SetExchange(exchange, rmq.ExchangeTopic)
	c.SetRoutingKey("sub1")
	c.SetQueueName("giterlab-queue-sub1")
	c.SetQueueType(rmq.QueueTypeClassic)
	c.SetQueueDurable(true)
	c.SettQueuAutoDelete(false)
	c.SetDeliveryMode(rmq.Persistent)
	c.SetQos(10)
	c.Info()

	// 消息订阅
	go func() {
		rmq.Subscribe(func(msg []byte) {
			defer func() {
				// recover panic
				if err := recover(); err != nil {
					GLog.Error("[sub1] panic, %s", err)
				}
			}()

			GLog.Debug("[sub1] %s", string(msg))
		}, c)
	}()

	// wait a moment
	select {}
}
