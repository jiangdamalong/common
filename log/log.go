package log

import (
	"bufio"
	"common"
	"fmt"
	"os"
	"path"
	"pool"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Level uint32

const (
	TimestampFormat = "2006-01-02 15:04:05.000"
)

const (
	BEGIN_LOG_LEVEL = iota
	PanicLevel
	FatalLevel
	ErrorLevel
	WarnLevel
	InfoLevel
	DebugLevel
	FlowLevel
	StatLevel
	RPCLevel
	StatRPCLevel
	InitLevel
	MAX_LEVEL
)

const (
	DEFAULT_BUF_IO_SIZE = 8196
)

func LevelString(level int) string {
	switch level {
	case ErrorLevel:
		return "error"
	case WarnLevel:
		return "warning"
	case InfoLevel:
		return "info"
	case DebugLevel:
		return "debug"
	case FlowLevel:
		return "flow"
	case StatLevel:
		return "stat"
	case RPCLevel:
		return "rrpc"
	case InitLevel:
		return "init"
	case FatalLevel:
		return "fatal"
	case PanicLevel:
		return "panic"
	case StatRPCLevel:
		return "rpc"
	}

	return "unknown"
}

type LogContext struct {
	logtime time.Time
	headers []string
	sfmt    string
	isfmt   int
	buf     string
	values  []interface{}
}

type LogHandler struct {
	level    int
	fileName string
	dir      string
	path     string
	fileIo   *os.File
	iobuf    *bufio.Writer
	bufChan  chan *LogContext

	timeDay time.Time
}

type pcAddrInfo struct {
	funcName string
	fileName string
	funcInfo string
}

type LogManager struct {
	mh        [MAX_LEVEL]LogHandler
	runFlag   int
	objpool   pool.ObjPool
	climit    int
	flushLock int32
	startLock int32
	maxLevel  int
	prefix    string

	printSwitch int
	exitChan    chan int

	IsPreFmt bool

	addrLock  sync.RWMutex
	pcAddrMap map[uint64]pcAddrInfo
}

var StLogger = LogManager{IsPreFmt: true, maxLevel: InfoLevel}

//限制单次写入条数，清空后退出
func (h *LogHandler) writeLog(climit int) int {
	var cnt = 0

for_read_channel:
	for {
		if climit > 0 && climit <= cnt {
			break
		}
		select {
		case buf_st := <-h.bufChan:
			h.changeFile(buf_st.logtime)
			h.iobuf.WriteString(strings.Join(buf_st.headers, string(0x1)))
			h.iobuf.WriteByte(0x1)
			if StLogger.IsPreFmt == true {
				h.iobuf.WriteString(buf_st.buf)
				h.iobuf.WriteString("\r\n")
			} else {
				if buf_st.isfmt == 0 {
					fmt.Fprintln(h.iobuf, buf_st.values...)
				} else {
					fmt.Fprintf(h.iobuf, buf_st.sfmt, buf_st.values...)
					h.iobuf.WriteString("\r\n")
				}
			}
			cnt++
			StLogger.objpool.Free(buf_st)
			continue
		default:
			break for_read_channel
		}
	}
	return cnt
}

func (h *LogHandler) getLogFileName(dtime time.Time, dir string) string {
	timeStr := dtime.Format("01-02")
	filepath := dir + timeStr + h.fileName
	return filepath
}

func (h *LogHandler) openFile(dtime time.Time, dir string) int {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return -1
	}
	h.dir = dir
	h.path = h.getLogFileName(dtime, h.dir)
	fd, erropen := os.OpenFile(h.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if erropen != nil {
		return -2
	}
	h.fileIo = fd
	h.iobuf = bufio.NewWriterSize(h.fileIo, DEFAULT_BUF_IO_SIZE)
	return 0
}

//定期重开文件，避免异常导致丢失过多日志
func (h *LogHandler) reopenFile() int {
	h.iobuf.Flush()
	h.fileIo.Close()

	if err := os.MkdirAll(h.dir, 0755); err != nil {
		return -1
	}
	fd, erropen := os.OpenFile(h.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if erropen != nil {
		return -2
	}

	h.fileIo = fd
	if h.iobuf != nil {
		h.iobuf.Reset(h.fileIo)
	} else {
		h.iobuf = bufio.NewWriterSize(h.fileIo, DEFAULT_BUF_IO_SIZE)
	}
	return 0
}

//日期变更，change file
func (h *LogHandler) changeFile(ntime time.Time) {
	if ntime.Day() != h.timeDay.Day() {
		h.timeDay = ntime
		h.path = h.getLogFileName(h.timeDay, h.dir)
		h.reopenFile()
	}
}

func (h *LogHandler) writeLogBuff(log *LogContext) int {
	h.bufChan <- log
	return 0
}

func (hs *LogManager) init(dir string, chan_len int) int {
	hs.objpool.InitRv(reflect.TypeOf((*LogContext)(nil)), chan_len*MAX_LEVEL)

	for i := BEGIN_LOG_LEVEL + 1; i < MAX_LEVEL; i++ {
		hs.mh[i].bufChan = make(chan *LogContext, chan_len)
		hs.mh[i].fileName = hs.prefix + "_" + LevelString(i) + ".log"
		hs.mh[i].timeDay = time.Now()
		ret := hs.mh[i].openFile(hs.mh[i].timeDay, dir)
		if ret < 0 {
			return ret - i*100
		}
	}
	hs.exitChan = make(chan int, 0)
	return 0
}

func (hs *LogManager) runFlush(climit int) {
	defer func() {
		atomic.SwapInt32(&hs.flushLock, 0)
		if hs.runFlag == common.FLAG_EXITED || hs.runFlag == common.FLAG_PANIC {
			return
		}
		hs.runFlag = common.FLAG_PANIC

		hs.mh[PanicLevel].fileIo.WriteString(string(debug.Stack()))
		if err := recover(); err != nil {
		}
	}()
	//加锁，仅允许一个线程执行
	if atomic.CompareAndSwapInt32(&hs.flushLock, 0, 1) == false {
		return
	}
	hs.runFlag = common.FLAG_RUNNING
	tnow := time.Now()
	tpre_flush := time.Now()
	tpre_reopen := time.Now()
	flush_index := uint32(0)
	reopen_index := uint32(0)
	for {
		select {
		case <-hs.exitChan:
			hs.runFlag = common.FLAG_EXITED
			return
		default:
		}
		if hs.runFlag == common.FLAG_BEGIN_EXIT {
			hs.runFlag = common.FLAG_EXITED
			break
		}
		tnow = time.Now()
		//100ms执行一次flush到文件
		if tnow.Sub(tpre_flush) > time.Millisecond*100 {
			tpre_flush = tnow
			flush_index++
			index := flush_index%(MAX_LEVEL-2) + 1
			hs.mh[index].iobuf.Flush()
		}

		//1s执行一个文件的重开
		if tnow.Sub(tpre_reopen) > time.Second*1 {
			tpre_reopen = tnow
			reopen_index++
			index := reopen_index%(MAX_LEVEL-2) + 1
			hs.mh[index].reopenFile()
		}

		cnt := 0
		//逐个刷入文件
		for i := BEGIN_LOG_LEVEL + 1; i < MAX_LEVEL; i++ {
			cnt += hs.mh[i].writeLog(climit)
		}
		//视情况让出cpu时间
		if cnt == 0 {
			time.Sleep(time.Millisecond * 50)
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}
}

//退出时刷新日志
func (hs *LogManager) ExitFlushLog(climit int) {
	hs.exitChan <- 1

	for i := BEGIN_LOG_LEVEL + 1; i < MAX_LEVEL; i++ {
		hs.mh[i].writeLog(climit)
		hs.mh[i].iobuf.Flush()
		hs.mh[i].fileIo.Close()
	}
}

func (hs *LogManager) Start(dir string, prefix string, climit int, chan_len int) int {
	//仅允许一次初始化
	dir = dir + "/"
	if atomic.CompareAndSwapInt32(&hs.startLock, 0, 1) == false {
		return -1
	}
	hs.pcAddrMap = make(map[uint64]pcAddrInfo)

	if prefix == "" {
		prefix = "bus"
	}
	hs.prefix = prefix
	ret := hs.init(dir, chan_len)
	if ret < 0 {
		return ret
	}
	hs.runFlag = common.FLAG_RUNNING
	hs.climit = climit

	go hs.runFlush(climit)
	return 0
}

func (hs *LogManager) writeLog(level int, stack int, isfmt int, sfmt string, logvals []interface{}) int {
	if hs.startLock == 0 {
		hs.Start("sync_log", hs.prefix, 300, 10000)
	}

	if hs.CheckLevel(level) == false {
		return 0
	}

	if hs.runFlag == common.FLAG_PANIC {
		go hs.runFlush(hs.climit)
	}

	pc, _, _, _ := runtime.Caller(stack + 1)

	addr := uint64(pc)
	funcInfo := ""
	hs.addrLock.RLock()
	if pcInfo, ok := hs.pcAddrMap[addr]; ok {
		funcInfo = pcInfo.funcInfo
		hs.addrLock.RUnlock()
	} else {
		hs.addrLock.RUnlock()
		pc, file, line, ok := runtime.Caller(stack + 1)
		//_, file = path.Split(file)
		aFile := strings.Split(file, "vendor/")
		file = aFile[len(aFile)-1]
		aFile = strings.Split(file, "/src/")
		file = aFile[len(aFile)-1]
		aFile = strings.Split(file, "/trunk/")
		file = aFile[len(aFile)-1]
		aFile = strings.Split(file, "/mod/")
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
			hs.addrLock.Lock()
			hs.pcAddrMap[addr] = p
			hs.addrLock.Unlock()
		}
	}

	/*for i, v := range logvals {
		if stringer, ok := v.(fmt.Stringer); ok {
			logvals[i] = stringer.String()
		}
	}*/

	logst := hs.AllocCtx()
	logst.logtime = time.Now()
	if StLogger.IsPreFmt == false {
		logst.values = logvals
		logst.isfmt = isfmt
		logst.sfmt = sfmt
	} else {
		if isfmt == 0 {
			logst.buf = fmt.Sprint(logvals...)
		} else {
			logst.buf = fmt.Sprintf(sfmt, logvals...)
		}
	}
	logst.headers = []string{logst.logtime.Format(TimestampFormat), funcInfo}
	ret := hs.mh[level].writeLogBuff(logst)
	return ret
}

func (hs *LogManager) WriteLog(level int, stack int, logvals ...interface{}) int {
	if hs.printSwitch == 1 {
		fmt.Println(logvals...)
		fmt.Printf("\r\n")
	}
	return hs.writeLog(level, stack+1, 0, "", logvals)
}

func (hs *LogManager) WriteLogf(level int, stack int, sfmt string, logvals ...interface{}) int {
	if hs.printSwitch == 1 {
		fmt.Printf(sfmt, logvals...)
		fmt.Printf("\r\n")
	}
	return hs.writeLog(level, stack+1, 1, sfmt, logvals)
}

func (hs *LogManager) WriteLogTest(level int, log string, t time.Time) int {
	if level >= MAX_LEVEL || level <= BEGIN_LOG_LEVEL {
		return -1
	}
	if hs.runFlag == common.FLAG_PANIC {
		go hs.runFlush(hs.climit)
	}
	logst := new(LogContext)
	logst.logtime = t
	vals := []interface{}{log}
	logst.values = vals
	ret := hs.mh[level].writeLogBuff(logst)
	return ret
}

func (hs *LogManager) Write(level int, logctx *LogContext) {
	if level >= MAX_LEVEL || level <= BEGIN_LOG_LEVEL {
		return
	}
	if hs.runFlag == common.FLAG_PANIC {
		go hs.runFlush(hs.climit)
	}
	hs.mh[level].writeLogBuff(logctx)
}

func (hs *LogManager) AllocCtx() *LogContext {
	return hs.objpool.Alloc().(*LogContext)
}

func (hs *LogManager) CheckLevel(level int) bool {
	if level == StatRPCLevel {
		return true
	}
	if level >= MAX_LEVEL || level <= BEGIN_LOG_LEVEL {
		return false
	}
	if level > hs.maxLevel {
		return false
	}
	/*if cap(hs.mh[level].bufChan) <= len(hs.mh[level].bufChan) {
		return false
	}*/
	return true
}

func (hs *LogManager) SetLevel(level int) {
	hs.maxLevel = level
}

func (hs *LogManager) OpenPrint() {
	hs.printSwitch = 1
}

func (hs *LogManager) ClosePrint() {
	hs.printSwitch = 0
}
