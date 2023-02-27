package log

import (
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type RpcLog struct {
	loglevel int
	logflag  int
	keymaps  map[string]interface{}
	keyvstr  string
	keys     []string
}

func (r *RpcLog) Init() {
	r.keyvstr = ""
	r.logflag = 0
	r.keymaps = map[string]interface{}{}
	r.keys = nil
}

func (r *RpcLog) Clear() {
	r.keyvstr = ""
	r.logflag = 0
	r.keymaps = nil
	r.keys = nil
}

func (r *RpcLog) SetKey(key string, value interface{}) int {
	if r.keymaps == nil {
		r.keymaps = make(map[string]interface{})
	}
	if r.keymaps[key] == nil {
		r.keys = append(r.keys, key)
	}
	r.keymaps[key] = value
	r.keyvstr = ""
	return 0
}

func (r *RpcLog) generateKeyStr() {
	keyvs := make([]string, len(r.keys))
	for i, v := range r.keys {
		keyvs[i] = fmt.Sprintf("%s=%+v", v, r.keymaps[v])
	}
	r.keyvstr = strings.Join(keyvs, string(0x2))
}

func (r *RpcLog) SetLogLevel(level int) {
	r.loglevel = level
	StLogger.maxLevel = level
}

func (r *RpcLog) SetLogSwitch(s int) {
	r.loglevel = s
}

func (r *RpcLog) log(isfmt int, sfmt string, level int, stackLevel int, value []interface{}) {
	if level > r.loglevel && r.logflag == 0 {
		return
	}
	//检查level chan是否用满，如果channel满，提前返回
	if StLogger.CheckLevel(level) == false {
		return
	}
	if r.keyvstr == "" {
		r.generateKeyStr()
	}
	var logctx = StLogger.AllocCtx()
	logctx.logtime = time.Now()
	/*logctx.values = value
	logctx.isfmt = isfmt
	logctx.sfmt = sfmt*/
	logctx.logtime = time.Now()
	if StLogger.IsPreFmt == false {
		logctx.values = value
		logctx.isfmt = isfmt
		logctx.sfmt = sfmt
	} else {
		if isfmt == 0 {
			logctx.buf = fmt.Sprint(value...)
		} else {
			logctx.buf = fmt.Sprintf(sfmt, value...)
		}
	}
	/*pc, file, line, ok := runtime.Caller(stackLevel + 1)
	_, file = path.Split(file)
	funcInfo := ""
	if ok == true {
		rfunc := runtime.FuncForPC(pc)
		funcName := ""
		if rfunc != nil {
			funcName = rfunc.Name()
		}
		funcInfo = file + " " + strconv.Itoa(line) + " " + funcName
	}*/

	pc, _, _, _ := runtime.Caller(stackLevel + 1)

	addr := uint64(pc)
	funcInfo := ""
	StLogger.addrLock.RLock()
	if pcInfo, ok := StLogger.pcAddrMap[addr]; ok {
		funcInfo = pcInfo.funcInfo
		StLogger.addrLock.RUnlock()
	} else {
		StLogger.addrLock.RUnlock()
		pc, file, line, ok := runtime.Caller(stackLevel + 1)
		//_, file = path.Split(file)
		aFile := strings.Split(file, "vendor/")
		file = aFile[len(aFile)-1]
		aFile = strings.Split(file, "/src/")
		file = aFile[len(aFile)-1]
		if ok == true {
			var p pcAddrInfo
			rfunc := runtime.FuncForPC(pc)
			funcName := ""
			if rfunc != nil {
				funcName = rfunc.Name()
				_, funcName = path.Split(funcName)
			}
			funcInfo = file + " " + strconv.Itoa(line) + " " + funcName
			p.funcInfo = funcInfo
			StLogger.addrLock.Lock()
			StLogger.pcAddrMap[addr] = p
			StLogger.addrLock.Unlock()
		}
	}

	logctx.headers = []string{logctx.logtime.Format(TimestampFormat), LevelString(level), funcInfo, r.keyvstr}
	StLogger.Write(level, logctx)
}

func (r *RpcLog) Log(level int, value ...interface{}) {
	if level > r.loglevel && r.logflag == 0 {
		return
	}
	if StLogger.printSwitch == 1 {
		fmt.Println(value)
		fmt.Printf("\r\n")
	}
	r.log(0, "", level, 1, value)
}

func (r *RpcLog) Logf(level int, sfmt string, value ...interface{}) {
	if level > r.loglevel && r.logflag == 0 {
		return
	}
	if StLogger.printSwitch == 1 {
		fmt.Printf(sfmt, value)
		fmt.Printf("\r\n")
	}
	r.log(1, sfmt, level, 1, value)
}

func (r *RpcLog) logfInStack(level int, sfmt string, stackLevel int, value ...interface{}) {
	if level > r.loglevel && r.logflag == 0 {
		return
	}
	if StLogger.printSwitch == 1 {
		fmt.Printf(sfmt, value)
		fmt.Printf("\r\n")
	}
	r.log(1, sfmt, level, stackLevel+1, value)
}

func (r *RpcLog) logInStack(level int, stackLevel int, value ...interface{}) {
	if level > r.loglevel && r.logflag == 0 {
		return
	}
	if StLogger.printSwitch == 1 {
		fmt.Println(value)
		fmt.Printf("\r\n")
	}
	r.log(0, "", level, stackLevel+1, value)
}

func (r *RpcLog) LogFlow(value ...interface{}) {
	if FlowLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(FlowLevel, 1, value...)
}

func (r *RpcLog) LogFlowf(sfmt string, value ...interface{}) {
	if FlowLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(FlowLevel, sfmt, 1, value...)
}

func (r *RpcLog) LogDebug(value ...interface{}) {
	if DebugLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(DebugLevel, 1, value...)
}

func (r *RpcLog) LogDebugf(sfmt string, value ...interface{}) {
	if DebugLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(DebugLevel, sfmt, 1, value...)
}

func (r *RpcLog) LogInfo(value ...interface{}) {
	if InfoLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(InfoLevel, 1, value...)
}

func (r *RpcLog) LogInfof(sfmt string, value ...interface{}) {
	if InfoLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(InfoLevel, sfmt, 1, value...)
}

func (r *RpcLog) LogError(value ...interface{}) {
	if ErrorLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(ErrorLevel, 1, value...)
}

func (r *RpcLog) LogErrorf(sfmt string, value ...interface{}) {
	if ErrorLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(ErrorLevel, sfmt, 1, value...)
}

func (r *RpcLog) LogFatal(value ...interface{}) {
	if FatalLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(FatalLevel, 1, value...)
}

func (r *RpcLog) LogFatalf(sfmt string, value ...interface{}) {
	if FatalLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(FatalLevel, sfmt, 1, value...)
}

func (r *RpcLog) Flow(value ...interface{}) {
	if FlowLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(FlowLevel, 1, value...)
}

func (r *RpcLog) Flowf(sfmt string, value ...interface{}) {
	if FlowLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(FlowLevel, sfmt, 1, value...)
}

func (r *RpcLog) Debug(value ...interface{}) {
	if DebugLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(DebugLevel, 1, value...)
}

func (r *RpcLog) Debugf(sfmt string, value ...interface{}) {
	if DebugLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(DebugLevel, sfmt, 1, value...)
}

func (r *RpcLog) Info(value ...interface{}) {
	if InfoLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(InfoLevel, 1, value...)
}

func (r *RpcLog) Infof(sfmt string, value ...interface{}) {
	if InfoLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(InfoLevel, sfmt, 1, value...)
}

func (r *RpcLog) Error(value ...interface{}) {
	if ErrorLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(ErrorLevel, 1, value...)
}

func (r *RpcLog) Errorf(sfmt string, value ...interface{}) {
	if ErrorLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(ErrorLevel, sfmt, 1, value...)
}

func (r *RpcLog) Fatal(value ...interface{}) {
	if FatalLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logInStack(FatalLevel, 1, value...)
}

func (r *RpcLog) Fatalf(sfmt string, value ...interface{}) {
	if FatalLevel > r.loglevel && r.logflag == 0 {
		return
	}
	r.logfInStack(FatalLevel, sfmt, 1, value...)
}
