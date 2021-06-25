package main

import (
	"github.com/GiterLab/rmq"
	"github.com/astaxie/beego/logs"
)

// GLog 全局日志变量
var GLog *logs.BeeLogger

func main() {
	// 设置日志
	GLog = logs.NewLogger(10000)
	GLog.SetLogger("console", `{"level":7}`)
	GLog.EnableFuncCallDepth(true)
	GLog.SetLogFuncCallDepth(3)

	rmq.Debug(true)
	rmq.SetUserDebug(func(format string, level int, v ...interface{}) {
		switch level {
		case rmq.LevelInformational:
			GLog.Info(format, v...)
		case rmq.LevelError:
			GLog.Error(format, v...)
		}
	})
	rmq.TraceInfo("debug test logs info %s, %s", "a", "b")
	rmq.TraceError("debug test logs error %s, %s", "a", "b")
}
