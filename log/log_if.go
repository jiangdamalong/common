package log

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func stringToLogLevel(level string) int {
	switch level {
	case "fatal":
		return FatalLevel
	case "error":
		return ErrorLevel
	case "warn":
		return WarnLevel
	case "warning":
		return WarnLevel
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	}
	return InitLevel
}

func Panic(v ...interface{}) {
	StLogger.WriteLog(PanicLevel, 1, v...)
}

func Panicf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(PanicLevel, 1, sfmt, v...)
}

func Error(v ...interface{}) {
	StLogger.WriteLog(ErrorLevel, 1, v...)
}

func Errorf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(ErrorLevel, 1, sfmt, v...)
}

func Fatal(v ...interface{}) {
	StLogger.WriteLog(FatalLevel, 1, v...)
}

func Fatalf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(FatalLevel, 1, sfmt, v...)
}

func Warn(v ...interface{}) {
	StLogger.WriteLog(WarnLevel, 1, v...)
}

func Warnf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(WarnLevel, 1, sfmt, v...)
}

func Info(v ...interface{}) {
	StLogger.WriteLog(InfoLevel, 1, v...)
}

func Infof(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(InfoLevel, 1, sfmt, v...)
}

func Flow(v ...interface{}) {
	StLogger.WriteLog(FlowLevel, 1, v...)
}

func Flowf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(FlowLevel, 1, sfmt, v...)
}

func Debug(v ...interface{}) {
	StLogger.WriteLog(DebugLevel, 1, v...)
}

func Debugf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(DebugLevel, 1, sfmt, v...)
}

func RpcStat(v ...interface{}) {
	StLogger.WriteLog(StatRPCLevel, 1, v...)
}

func RpcStatf(sfmt string, v ...interface{}) {
	StLogger.WriteLogf(StatRPCLevel, 1, sfmt, v...)
}

func StartV(dir string, prefix string, climit int, chan_len int) {
	StLogger.Start(dir, prefix, climit, chan_len)
}

func Start(dir string, prefix string) {
	StLogger.Start(dir, prefix, 100, 10000)
}

func SetLevel(level int) {
	StLogger.SetLevel(level)
}

func SetLevelString(level string) {
	StLogger.SetLevel(stringToLogLevel(level))
}

func Print(v ...interface{}) {
	fmt.Println(v...)
	Debug(v...)
}

func Println(v ...interface{}) {
	Print(v...)
}

func Printf(sfmt string, v ...interface{}) {
	fmt.Printf(sfmt+"\r\n", v...)
}

func getLogHead() string {
	pc, file, line, ok := runtime.Caller(2)
	_, file = path.Split(file)
	funcInfo := ""
	if ok == true {
		rfunc := runtime.FuncForPC(pc)
		funcName := ""
		if rfunc != nil {
			funcName = rfunc.Name()
			_, funcName = path.Split(funcName)
		}
		funcInfo = file + " " + strconv.Itoa(line) + " " + funcName
	}

	logTime := time.Now()
	headers := []string{logTime.Format(TimestampFormat), funcInfo, " "}
	return strings.Join(headers, string(0x1))
}

func Initf(sfmt string, v ...interface{}) {
	fd, err := os.OpenFile("./sync_log/initing.log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf(getLogHead())
		fmt.Printf(sfmt, v...)
		fmt.Printf("\n")
	} else {
		fmt.Fprint(fd, getLogHead())
		fmt.Fprintf(fd, sfmt, v...)
		fmt.Fprintf(fd, "\n")
		fd.Close()
	}
}

func Init(v ...interface{}) {
	fd, err := os.OpenFile("./sync_log/initing.log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf(getLogHead())
		fmt.Print(v...)
		fmt.Printf("\n")
	} else {
		fmt.Fprint(fd, getLogHead())
		fmt.Fprint(fd, v...)
		fmt.Fprintf(fd, "\n")
		fd.Close()
	}
}

func OpenPrint() {
	StLogger.OpenPrint()
}

func ClosePrint() {
	StLogger.ClosePrint()
}
