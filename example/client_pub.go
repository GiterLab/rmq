package main

import (
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
	exchange = "giterlabrmq"
)

func main() {
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
	c.SetExchange(exchange, rmq.ExchangeFanout)
	c.SetRoutingKey("giterlab")
	c.SetQueueName("giterlab-queue")
	c.Info()
	// rmq.Publish(messages, c)
}
