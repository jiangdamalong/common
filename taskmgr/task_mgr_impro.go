package taskmgr

import (
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiangdamalong/common"
	"github.com/jiangdamalong/common/log"
)

type HTaskNode struct {
	args     []reflect.Value
	callFunc reflect.Value
	resChan  chan int

	hashKey   uint64
	isOrder   bool
	isSync    bool
	procIndex uint64

	m *mapStruct

	next *HTaskNode
}

type TaskCtx struct {
	taskId  int //任务类型
	hashKey uint64
	isOrder bool
	f       interface{}
	parent  *TaskCtx //父调用
}

func (tc *TaskCtx) Print() {
	for tc != nil {
		log.Printf("tc %+v", tc)
		tc = tc.parent
	}
}

type TaskHashNode struct {
	//sync.RWMutex
	isRunning bool //是否在执行中，或者在taskChan
	//isDeleted  bool       //是否已删除
	fQueueHead *HTaskNode //等待队列头部，头取
	fQueueTail *HTaskNode //等待队列尾部，尾插

	next *TaskHashNode
}

//任务等待
type TaskWaiter interface {
	Add(delta int)
	Done()
}

type DefaultWaiter struct {
}

func (w *DefaultWaiter) Add(delta int) {
}

func (w *DefaultWaiter) Done() {
}

var Waiter TaskWaiter = &DefaultWaiter{}

type mapStruct struct {
	sync.Mutex
	hashMap map[uint64]*TaskHashNode

	taskChan     chan *HTaskNode
	pGroutineNum int32
	exitChan     chan int

	hashNodeFree *TaskHashNode
	nodeFree     *HTaskNode

	nodeLock sync.Mutex
	hashLock sync.Mutex
}

type TaskMgr struct {
	//WaitTimeOut uint64

	maxThreadNum int32
	//threadNum     int32
	minThreadNum int32
	idleNum      int32
	threadLock   sync.Mutex
	isFixNum     bool
	taskId       int
	procIndex    uint64

	hashMaps []*mapStruct //执行状态以及等待队列

	procNum uint64

	nodeSeq uint32
	hashSeq uint32
}

func (tm *TaskMgr) onFunPanic(task *HTaskNode) {
	log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", task.args)
	if task.callFunc.Kind() != reflect.Func {
		log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", "func type error")
		return
	}
	f := runtime.FuncForPC(task.callFunc.Pointer())
	file, l := f.FileLine(f.Entry())
	log.StLogger.WriteLog(log.PanicLevel, 1, "get panic :", file, ":", l, " ", f.Name())
}

func (tm *TaskMgr) run(index uint64) {
	exitFlag := common.FLAG_RUNNING
	var task *HTaskNode
	task = nil
	procInfo := tm.hashMaps[index]

	defer func() {
		//atomic.AddInt32(&tm.threadNum, -1)
		atomic.AddInt32(&procInfo.pGroutineNum, -1)
		if exitFlag == common.FLAG_BEGIN_EXIT {
			return
		} else {
			log.StLogger.WriteLog(log.PanicLevel, 1, string(debug.Stack()))
			if task != nil {
				if task.isSync == true {
					task.resChan <- TASK_RET_HANDLE_PANIC
				}
				tm.onFunPanic(task)
			}
			if err := recover(); err != nil {
				log.StLogger.WriteLog(log.PanicLevel, 1, err)
			}
		}
		if task != nil {
			task = tm.onTaskDone(task)
			Waiter.Done()
			if task != nil {
				procInfo.taskChan <- task
			}
		}
	}()

	tm.threadLock.Lock()
	//atomic.AddInt32(&tm.threadNum, 1)
	atomic.AddInt32(&procInfo.pGroutineNum, 1)
	if procInfo.pGroutineNum > tm.maxThreadNum {
		exitFlag = common.FLAG_BEGIN_EXIT
		tm.threadLock.Unlock()
		return
	}
	tm.threadLock.Unlock()
for_run_routine:
	for {
		//var task *HTaskNode
		if task == nil {
			select {
			case task = <-procInfo.taskChan:
			case <-procInfo.exitChan:
				exitFlag = common.FLAG_BEGIN_EXIT
				break for_run_routine
			}
		}
		(task.callFunc).Call(task.args)
		if task.isSync == true {
			task.resChan <- 0
		}
		task = tm.onTaskDone(task)
		Waiter.Done()
	}
}

//协程数量超过min_num，len(task)小于1/4容量，或者task队列为空，触发回收
func (tm *TaskMgr) recycleRoutine() {
	for {
		for _, procInfo := range tm.hashMaps {
			if procInfo.pGroutineNum > tm.minThreadNum && len(procInfo.taskChan) == 0 {
				var recycleNum = procInfo.pGroutineNum - tm.minThreadNum
				/*if (int)(procInfo.pGroutineNum-tm.minThreadNum) < (int)(cap(procInfo.exitChan)) {
					recycleNum = (int)(procInfo.pGroutineNum - tm.minThreadNum)
				}*/
				for j := 0; j < int(recycleNum); j++ {
					procInfo.exitChan <- 1
				}
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (tm *TaskMgr) Start(maxThreadNum int, minThreadNum int, chanLen int, isFixNum bool, procNum int) {
	tm.minThreadNum = (int32)(minThreadNum)
	tm.maxThreadNum = (int32)(maxThreadNum)
	//tm.taskChans = make([]chan *HTaskNode, procNum)
	//tm.exitChans = make([]chan int, minThrea)
	tm.isFixNum = isFixNum

	tm.hashMaps = make([]*mapStruct, procNum)

	//tm.pGroutineNums = make([]int32, procNum)
	//tm.exitChans = make([]chan int, procNum)
	//tm.nodeFree = make([]*HTaskNode, procNum)
	//tm.hashNodeFree = make([]*TaskHashNode, procNum)
	//tm.nodeLock = make([]sync.Mutex, procNum)
	//tm.hashLock = make([]sync.Mutex, procNum)

	for i := 0; i < procNum; i++ {
		tm.hashMaps[i] = new(mapStruct)
		tm.hashMaps[i].hashMap = make(map[uint64]*TaskHashNode)
		tm.hashMaps[i].taskChan = make(chan *HTaskNode, chanLen)
		tm.hashMaps[i].exitChan = make(chan int, 0)
		tm.hashMaps[i].pGroutineNum = 0
	}
	tm.procNum = uint64(procNum)

	if tm.isFixNum == false {
		go tm.recycleRoutine()
	}
	//默认5s过期
	//tm.WaitTimeOut = 5 * 1000
	/*	for i := (int32)(0); i < tm.maxThreadNum; i++ {
		go tm.run()
	}*/
}

func (tm *TaskMgr) recycleNode(node *HTaskNode) {
	tm.nodeSeq++
	if tm.nodeSeq%64 == 0 {
		return
	}
	procInfo := node.m
	node.args = node.args[0:0]
	node.isOrder = false
	node.callFunc = reflect.Value{}

	procInfo.nodeLock.Lock()
	node.next = procInfo.nodeFree
	procInfo.nodeFree = node
	procInfo.nodeLock.Unlock()
	//tm.nodePool.Free(node)
}

func (tm *TaskMgr) getNode(procInfo *mapStruct) *HTaskNode {
	//procInfo := tm.hashMaps[index]
	procInfo.nodeLock.Lock()
	if procInfo.nodeFree == nil {
		procInfo.nodeLock.Unlock()
		t := new(HTaskNode)
		t.resChan = make(chan int, 1)
		return t
	} else {
		n := procInfo.nodeFree
		procInfo.nodeFree = procInfo.nodeFree.next
		procInfo.nodeLock.Unlock()
		return n
	}
}

func (tm *TaskMgr) recycleHashNode(node *TaskHashNode, m *mapStruct) {
	tm.hashSeq++
	if tm.hashSeq%64 == 0 {
		return
	}
	//procInfo := tm.hashMaps[index]
	node.isRunning = false

	//procInfo.hashLock.Lock()
	node.next = m.hashNodeFree
	m.hashNodeFree = node
	//procInfo.hashLock.Unlock()
	//node.fQueue = make(list.List)
}

func (tm *TaskMgr) getHashNode(procInfo *mapStruct) *TaskHashNode {
	//procInfo := tm.hashMaps[index]
	//procInfo.hashLock.Lock()
	if procInfo.hashNodeFree == nil {
		//procInfo.hashLock.Unlock()
		return new(TaskHashNode)
	} else {
		n := procInfo.hashNodeFree
		procInfo.hashNodeFree = procInfo.hashNodeFree.next
		//procInfo.hashLock.Unlock()
		return n
	}
}

func (tm *TaskMgr) onTaskDone(task *HTaskNode) *HTaskNode {
	//defer tm.recycleNode(task)
	//defer Waiter.Done()
	if task.isOrder == false {
		tm.recycleNode(task)
		return nil
	}
	var m = task.m
	m.Lock()
	hashNode, ok := m.hashMap[task.hashKey]
	if !ok || hashNode == nil {
		panic("no node find error")
	}
	//如果等待队列为空，则删掉hashNode
	//否则取出头部元素，放入执行队列
	if hashNode.fQueueHead == nil {
		delete(m.hashMap, task.hashKey)
		tm.recycleHashNode(hashNode, task.m)
		m.Unlock()
		//fmt.Printf("clean queue\n")
	} else {
		hashNode.isRunning = true
		nTask := hashNode.fQueueHead
		hashNode.fQueueHead = nTask.next
		if hashNode.fQueueHead == nil {
			hashNode.fQueueTail = nil
		}
		m.Unlock()
		select {
		case m.taskChan <- nTask:
		default:
			tm.recycleNode(task)
			return nTask
		}
		//fmt.Printf("add wait task\n")
	}
	tm.recycleNode(task)
	return nil
}

func (tm *TaskMgr) addTask(task *HTaskNode) {
	var m = task.m
	procInfo := tm.hashMaps[task.procIndex]
	m.Lock()
	hashNode, ok := m.hashMap[task.hashKey]
	if !ok {
		hashNode = tm.getHashNode(task.m)
		hashNode.fQueueTail = nil
		hashNode.fQueueHead = nil
		hashNode.isRunning = true
		m.hashMap[task.hashKey] = hashNode
		m.Unlock()
		procInfo.taskChan <- task
	} else {
		if hashNode.isRunning == false && hashNode.fQueueHead == nil {
			hashNode.isRunning = true
			m.Unlock()
			procInfo.taskChan <- task
		} else {
			hashNode.isRunning = true
			task.next = nil
			if hashNode.fQueueTail != nil {
				hashNode.fQueueTail.next = task
				hashNode.fQueueTail = task
			} else {
				hashNode.fQueueTail = task
				hashNode.fQueueHead = hashNode.fQueueTail
				task.next = nil
			}
			m.Unlock()
		}
		//m.Unlock()
	}
}

func checkFunc(f *reflect.Value) {
	if f.Kind() != reflect.Func {
		panic("get invalid func ptr")
	}
}

func (tm *TaskMgr) callGo(isOrder bool, hashKey uint64, cf interface{}, args ...interface{}) error {
	//fmt.Printf("and func %+v\n", hashKey)
	//task := tm.nodePool.Alloc().(*HTaskNode)
	//tm.procIndex = hashKey
	procIndex := hashKey % tm.procNum
	procInfo := tm.hashMaps[procIndex]
	task := tm.getNode(procInfo)
	task.procIndex = procIndex
	//task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args = append(task.args, reflect.ValueOf(args[i]))
	}

	task.callFunc = reflect.ValueOf(cf)
	//checkFunc(&task.callFunc)
	task.hashKey = hashKey
	task.isOrder = isOrder
	task.isSync = false
	//fmt.Printf("tm.procNum %+v task.hashKey %+v\n", task.hashKey, task.hashKey%uint64(tm.procNum))
	task.m = procInfo
	Waiter.Add(1)

	if (procInfo.pGroutineNum < tm.maxThreadNum && len(procInfo.taskChan) >= cap(procInfo.taskChan)) || procInfo.pGroutineNum < tm.minThreadNum {
		go tm.run(procIndex)
	}
	//task.resChan = make(chan int, 1)
	if !isOrder {
		procInfo.taskChan <- task
	} else {
		tm.addTask(task)
	}
	return nil
}

func (tm *TaskMgr) callGroup(isOrder bool, hashKey uint64, cf interface{}, args ...interface{}) chan int {
	//fmt.Printf("and func %+v\n", hashKey)
	//task := tm.nodePool.Alloc().(*HTaskNode)
	//tm.procIndex = hashKey
	procIndex := hashKey % tm.procNum
	procInfo := tm.hashMaps[procIndex]
	task := tm.getNode(procInfo)
	task.procIndex = procIndex
	//task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args = append(task.args, reflect.ValueOf(args[i]))
	}

	task.callFunc = reflect.ValueOf(cf)
	checkFunc(&task.callFunc)
	task.hashKey = hashKey
	task.isOrder = isOrder
	task.isSync = true

	task.m = procInfo
	Waiter.Add(1)

	if (procInfo.pGroutineNum < tm.maxThreadNum && len(procInfo.taskChan) >= cap(procInfo.taskChan)) || procInfo.pGroutineNum < tm.minThreadNum {
		go tm.run(procIndex)
	}

	//task.resChan = make(chan int, 1)
	if !isOrder {
		procInfo.taskChan <- task
	} else {
		tm.addTask(task)
	}
	return task.resChan
}

func (tm *TaskMgr) call(isOrder bool, hashKey uint64, cf interface{}, args ...interface{}) {
	//task := tm.nodePool.Alloc().(*HTaskNode)
	//tm.procIndex = hashKey
	procIndex := hashKey % tm.procNum
	procInfo := tm.hashMaps[procIndex]
	task := tm.getNode(procInfo)
	task.procIndex = procIndex
	//task.args = make([]reflect.Value, len(args))
	for i := 0; i < len(args); i++ {
		task.args = append(task.args, reflect.ValueOf(args[i]))
	}

	task.callFunc = reflect.ValueOf(cf)
	checkFunc(&task.callFunc)
	task.hashKey = hashKey
	task.isOrder = isOrder
	task.m = procInfo
	task.isSync = true
	Waiter.Add(1)

	if (procInfo.pGroutineNum < tm.maxThreadNum && len(procInfo.taskChan) >= cap(procInfo.taskChan)) || procInfo.pGroutineNum < tm.minThreadNum {
		go tm.run(procIndex)
	}

	task.resChan = make(chan int, 1)

	if !isOrder {
		procInfo.taskChan <- task
	} else {
		tm.addTask(task)
	}
	<-task.resChan

	//return result, retVal
}

//同步调用，需要维护调用链关系
func (tm *TaskMgr) callCtx(pctx *TaskCtx, isOrder bool, hash uint64, cf interface{}, args ...interface{}) {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = tm.taskId
	tc.parent = pctx
	tc.isOrder = isOrder
	tc.f = cf
	var rargs = make([]interface{}, 0, len(args)+1)
	rargs = append(rargs, &tc)
	rargs = append(rargs, args...)

	tm.call(isOrder, hash, cf, rargs...)
}

//异步调用，另开调用链
func (tm *TaskMgr) callCtxGo(isOrder bool, hash uint64, cf interface{}, args ...interface{}) {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = tm.taskId
	tc.parent = nil
	tc.isOrder = isOrder
	tc.f = cf
	var rargs = make([]interface{}, 0, len(args)+1)
	rargs = append(rargs, &tc)
	rargs = append(rargs, args...)
	tm.callGo(isOrder, hash, cf, rargs...)
}

//同步调用，需要维护调用链关系
func (tm *TaskMgr) callCtxGroup(pctx *TaskCtx, isOrder bool, hash uint64, cf interface{}, args ...interface{}) chan int {
	var tc TaskCtx
	tc.hashKey = hash
	tc.taskId = tm.taskId
	tc.parent = pctx
	tc.isOrder = isOrder
	tc.f = cf
	var rargs = make([]interface{}, 0, len(args)+1)
	rargs = append(rargs, &tc)
	rargs = append(rargs, args...)
	return tm.callGroup(isOrder, hash, cf, rargs...)
}
