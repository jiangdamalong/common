package taskmgr

import (
	"errors"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiangdamalong/common"
	"github.com/jiangdamalong/common/log"
	"github.com/jiangdamalong/common/pool"
)

const (
	TASK_RET_FRAME_TIMEOUT  = -10000001
	TASK_RET_HANDLE_TIMEOUT = -10000002
	TASK_RET_HANDLE_PANIC   = -10000003
	TASK_RET_HANDLE_UNDO    = -20000000
)

//type CallFunc func(req interface{}, resp interface{}) int
type CallFunc reflect.Value

type TaskNode struct {
	args     []reflect.Value
	callfunc *reflect.Value
	resChan  chan int

	result   int
	retValue []interface{}
}

type TaskGroup struct {
	nodes []*TaskNode
	tm    *TaskManager
}

func (tg *TaskGroup) Init(tm *TaskManager) {
	tg.nodes = nil
	tg.tm = tm
}

func (tg *TaskGroup) AddFunc(cf interface{}, args ...interface{}) {
	task := tg.tm.nodePool.Alloc().(*TaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callfunc := reflect.ValueOf(cf)
	task.callfunc = &callfunc
	task.result = TASK_RET_HANDLE_UNDO
	task.retValue = make([]interface{}, 0, 2)
	task.resChan = make(chan int, 1)
	tg.nodes = append(tg.nodes, task)
}

func (tg *TaskGroup) AddFuncS(cf interface{}, args ...interface{}) {
	tg.AddFunc(cf, args...)
}

func (tg *TaskGroup) CallFuncs() {
	tg.tm.callFuncs(tg)
}

func (tg *TaskGroup) CallFuncsS() {
	tg.tm.callFuncs(tg)
}

func (tm *TaskManager) callFuncs(tg *TaskGroup) {
	//retRes := make([]int, len(tg.nodes))

	for i := 0; i < len(tg.nodes); i++ {
		if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
			go tm.run()
		}
		tm.taskChan <- tg.nodes[i]
	}

	for i := 0; i < len(tg.nodes); i++ {
		<-tg.nodes[i].resChan
		//retRes[i] = tg.nodes[i].result
		tm.recycleNode(tg.nodes[i])
	}

	tg.nodes = nil
	return
}

type TaskManager struct {
	taskChan     chan *TaskNode
	exitChan     chan int
	maxThreadNum int32
	threadNum    int32
	minThreadNum int32
	idleNum      int32
	nodePool     pool.ObjPool
	threadLock   sync.Mutex
	isFixNum     bool
}

func (tm *TaskManager) run() {
	exitFlag := common.FLAG_RUNNING
	var task *TaskNode
	defer func() {
		atomic.AddInt32(&tm.threadNum, -1)
		if exitFlag == common.FLAG_BEGIN_EXIT {
			return
		} else {
			log.StLogger.WriteLog(log.PanicLevel, 1, string(debug.Stack()))
			if task != nil {
				task.resChan <- TASK_RET_HANDLE_PANIC
				log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", task.args)
			}
			//fmt.Printf("pacnic here %s\n", string(debug.Stack()))
			if err := recover(); err != nil {
				log.StLogger.WriteLog(log.PanicLevel, 1, err)
			}
		}
	}()

	tm.threadLock.Lock()
	atomic.AddInt32(&tm.threadNum, 1)
	if tm.threadNum > tm.maxThreadNum {
		exitFlag = common.FLAG_BEGIN_EXIT
		tm.threadLock.Unlock()
		return
	}
	tm.threadLock.Unlock()

for_run_routine:
	for {
		//var task *TaskNode
		select {
		case task = <-tm.taskChan:
		case <-tm.exitChan:
			exitFlag = common.FLAG_BEGIN_EXIT
			break for_run_routine
		}

		ret := (*(task.callfunc)).Call(task.args)

		task.result = 0
		for _, v := range ret {
			switch v.Kind() {
			case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
				if v.IsNil() {
					task.retValue = append(task.retValue, nil)
				} else {
					task.retValue = append(task.retValue, v.Interface())
				}
			default:
				task.retValue = append(task.retValue, v.Interface())
			}
		}

		task.resChan <- task.result
	}
}

//协程数量超过min_num，len(task)小于1/4容量，或者task队列为空，触发回收
func (tm *TaskManager) recycleRoutine() {
	for {
		if tm.threadNum > tm.minThreadNum && (len(tm.taskChan) < (cap(tm.taskChan))/4 || len(tm.taskChan) == 0) {
			var recycleNum = cap(tm.exitChan)
			if (int)(tm.threadNum-tm.minThreadNum) < (int)(cap(tm.exitChan)) {
				recycleNum = (int)(tm.threadNum - tm.minThreadNum)
			}
			for i := 0; i < recycleNum; i++ {
				tm.exitChan <- 1
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (tm *TaskManager) Start(maxThreadNum int, minThreadNum int, isFixNum bool) {
	tm.nodePool.InitRv(reflect.TypeOf((*TaskNode)(nil)), 1000)
	tm.minThreadNum = (int32)(minThreadNum)
	tm.maxThreadNum = (int32)(maxThreadNum)
	tm.taskChan = make(chan *TaskNode, maxThreadNum)
	tm.exitChan = make(chan int, minThreadNum)
	tm.isFixNum = isFixNum

	if tm.isFixNum == false {
		go tm.recycleRoutine()
	}
	/*	for i := (int32)(0); i < tm.maxThreadNum; i++ {
		go tm.run()
	}*/
}

func (tm *TaskManager) hstart(chlen int, isFixNum bool) {
	tm.nodePool.InitRv(reflect.TypeOf((*TaskNode)(nil)), 1000)
	tm.minThreadNum = (int32)(1)
	tm.maxThreadNum = (int32)(1)
	tm.taskChan = make(chan *TaskNode, chlen)
	tm.exitChan = make(chan int, 1)
	tm.isFixNum = isFixNum

	if tm.isFixNum == false {
		go tm.recycleRoutine()
	}
}

func (tm *TaskManager) recycleNode(node *TaskNode) {
	node.args = nil
	node.callfunc = nil
	node.retValue = nil
	tm.nodePool.Free(node)
}

//异步调用不等待，queue满即返回出错
func (tm *TaskManager) CallGo(cf interface{}, args ...interface{}) error {
	task := tm.nodePool.Alloc().(*TaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callfunc := reflect.ValueOf(cf)
	task.callfunc = &callfunc
	task.result = TASK_RET_HANDLE_UNDO

	if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
		go tm.run()
	}
	task.resChan = make(chan int, 1)
	select {
	case tm.taskChan <- task:
		return nil
	default:
		return errors.New("Task queue full")
	}
}

//异步调用，queue满需等待
func (tm *TaskManager) CallGoW(cf interface{}, args ...interface{}) error {
	task := tm.nodePool.Alloc().(*TaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callfunc := reflect.ValueOf(cf)
	task.callfunc = &callfunc
	task.result = TASK_RET_HANDLE_UNDO

	if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
		go tm.run()
	}
	task.resChan = make(chan int, 1)
	tm.taskChan <- task
	return nil
}

func (tm *TaskManager) Call(cf interface{}, args ...interface{}) (int, []interface{}) {
	task := tm.nodePool.Alloc().(*TaskNode)
	task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args[i] = reflect.ValueOf(args[i])
	}

	callfunc := reflect.ValueOf(cf)
	task.callfunc = &callfunc
	task.result = TASK_RET_HANDLE_UNDO

	if (tm.threadNum < tm.maxThreadNum && len(tm.taskChan) >= cap(tm.taskChan)) || tm.threadNum < tm.minThreadNum {
		go tm.run()
	}
	var retVal []interface{}
	task.resChan = make(chan int, 1)
	result := 0
	{
		tm.taskChan <- task
		<-task.resChan
		result = task.result
		retVal = task.retValue
		tm.recycleNode(task)
	}
	return result, retVal
}

func (tm *TaskManager) CallS(cf interface{}, args ...interface{}) (int, []interface{}) {
	return tm.Call(cf, args...)
}
