package rmq

import (
	"log"
)

const (
	LevelEmergency = iota
	LevelAlert
	LevelCritical
	LevelError
	LevelWarning
	LevelNotice
	LevelInformational
	LevelDebug
)

var debugEnable bool

type TraceFunc func(format string, level int, v ...interface{})

var DoFunc TraceFunc = nil

func init() {
	debugEnable = false
	log.SetPrefix("[RMQ] TRACE: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
}

func AddDebugFunc(f TraceFunc) {
	DoFunc = f
}

// Debug Enable debug
func Debug(enable bool) {
	debugEnable = enable
}
func TraceInfo(format string, v ...interface{}) {
	if debugEnable {
		log.Printf(format, v...)
	}
}
func TraceError(format string, v ...interface{}) {
	if DoFunc != nil {
		DoFunc(format, LevelError, v)
	}

	if debugEnable {
		log.Printf(format, v...)
	}

}

//Deprecated: use SetDebugger instead
func SetLogger(l ...interface{}) {

}
