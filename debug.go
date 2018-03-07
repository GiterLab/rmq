package rmq

import (
	"github.com/astaxie/beego/logs"
)

var debugEnable bool

// GLog debug log
var GLog *logs.BeeLogger

func init() {
	debugEnable = false
	GLog = logs.NewLogger(10000)
	GLog.SetLogger("console", `{"level":7}`)
	GLog.EnableFuncCallDepth(true)
	GLog.SetLogFuncCallDepth(3)
}

// Debug Enable debug
func Debug(enable bool) {
	debugEnable = enable
}

// SetLogger Set new logger
func SetLogger(l *logs.BeeLogger) {
	if l != nil {
		GLog = l
	}
}
