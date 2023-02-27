package etcdlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/client"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/jiangdamalong/common/log"
)

type RWLockType int

const (
	LockTypeRead RWLockType = iota
	LockTypeWrite
)

var LockTypes = map[RWLockType]string{
	LockTypeRead:  "read-lock",
	LockTypeWrite: "write-lock",
}

type LockState struct {
	Id       int    `json:"Id"`
	LockType string `json:"LockType"`
}

type EtcdRWLock struct {
	mtx      sync.Mutex
	client   ClientAPI
	dir      string
	name     string
	mykey    string
	svrId    int
	ttl      uint64
	content  string
	stopFunc context.CancelFunc
	holding  bool
	start    bool
	lockType RWLockType
	timeout  time.Duration
	lefttime time.Duration
	eventsCh chan LockEventType
}

func NewEtcdRWLock(endpoints []string, name string, server_id int, ttl uint64, lockType RWLockType) (EtcdLockInterface, error) {
	cfg := client.Config{
		Endpoints: endpoints,
		Transport: client.DefaultTransport,
	}

	c, err := client.New(cfg)
	if err != nil {
		log.Errorf("Init etcdClient error :%s", err.Error())
		return nil, err
	}
	clientAPI := client.NewKeysAPI(c)

	return &EtcdRWLock{client: clientAPI, name: name, svrId: server_id, ttl: ttl,
		dir:      "/rwlock",
		content:  "",
		mykey:    "",
		stopFunc: nil,
		holding:  false,
		start:    false,
		lockType: lockType,
		timeout:  0,
		lefttime: 0,
		eventsCh: make(chan LockEventType, 1)}, nil
}

func NewAsyncEtcdRWLock(endpoints []string, name string, server_id int, ttl uint64, lockType RWLockType) (AsyncEtcdLockInterface, error) {
	cfg := client.Config{
		Endpoints: endpoints,
		Transport: client.DefaultTransport,
	}

	c, err := client.New(cfg)
	if err != nil {
		log.Errorf("Init etcdClient error :%s", err.Error())
		return nil, err
	}
	clientAPI := client.NewKeysAPI(c)

	return &EtcdRWLock{client: clientAPI, name: name, svrId: server_id, ttl: ttl,
		dir:      "/rwlock",
		content:  "",
		mykey:    "",
		stopFunc: nil,
		holding:  false,
		start:    false,
		lockType: lockType,
		timeout:  0,
		lefttime: 0,
		eventsCh: make(chan LockEventType, 1)}, nil
}

func (e *EtcdRWLock) Acquire(ctx context.Context, timeout time.Duration) error {
	log.Debugf("Acquire Lock routine Lockname :%s, Timeout :%s", e.name, timeout.String())

	e.mtx.Lock()
	if e.start {
		e.mtx.Unlock()
		log.Errorf("Lock routine exist")
		return ErrRoutineExist
	}

	e.start = true
	child_ctx, cancel := context.WithCancel(ctx)
	e.stopFunc = cancel
	e.mtx.Unlock()

	go func() {
		i := 1
		for {
			if err := e.acquire(child_ctx, timeout); err == nil {
				log.Debugf("Lock acquire routine done")
				break
			} else {
				i++
				log.Errorf("Lock acquire routine start error")
				if i >= 3 {
					break
				}
			}
		}
	}()

	return nil
}

func (e *EtcdRWLock) acquire(ctx context.Context, timeout time.Duration) (ret error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1<<16)
			stackSize := runtime.Stack(buf, true)
			errMsg := fmt.Sprintf("Recovered from panic: %s", buf[:stackSize])
			log.Errorf("Lock routine panic : %s", errMsg)
			ret = errors.New(errMsg)
		}
	}()

	req := &LockState{Id: e.svrId, LockType: LockTypes[e.lockType]}
	if reqjson, err := json.Marshal(req); err != nil {
		log.Errorf("Json marshal error")
		return ErrJsonMsh
	} else {
		e.content = string(reqjson)
	}

	e.timeout = timeout
	e.lefttime = timeout

	_, err := e.prepare(ctx)
	if err != nil {
		log.Errorf("Prepare error %s error %s", e.name, err)
		e.eventsCh <- LockError
		return nil
	}

	var curCtx context.Context

	chklt := func() bool {
		if timeout != 0 {
			e.mtx.Lock()
			if e.lefttime <= 5*time.Millisecond {
				e.mtx.Unlock()
				e.eventsCh <- LockTimeout
				return false
			}
			curCtx, _ = context.WithTimeout(ctx, e.lefttime)
			e.mtx.Unlock()
		} else {
			curCtx = ctx
		}

		return true
	}

	for {
		var curIdx uint64
		var ret bool
		var err error
		if !chklt() {
			log.Debugf("%s timeout", e.name)
			break
		}

		ret, curIdx, err = e.testLock(curCtx)
		if ret == true {
			log.Debugf("RWLock %s lock SUCCESS", e.name)
			e.mtx.Lock()
			e.holding = true
			e.mtx.Unlock()
			e.eventsCh <- LockAcq
			break
		} else if err != nil {
			log.Errorf("RWLock error %s: %s", e.name, err.Error())
			e.eventsCh <- errToEvent(err)
			break
		}

		if !chklt() {
			log.Debugf("%s timeout", e.name)
			break
		}

		err = e.wait(curCtx, curIdx)
		if err != nil {
			log.Errorf("RWLock wait error %s: %s", e.name, err.Error())
			e.eventsCh <- errToEvent(err)
			break
		}
	}

	return nil
}

func (e *EtcdRWLock) wait(ctx context.Context, waitIdx uint64) error {
	if e.timeout != 0 {
		defer e.timeTrack(time.Now(), "wait")
	}

	dir := path.Join(e.dir, e.name)
	watcher := e.client.Watcher(dir, &client.WatcherOptions{AfterIndex: waitIdx, Recursive: true})
	_, err := watcher.Next(ctx)
	return err
}

func (e *EtcdRWLock) prepare(ctx context.Context) (uint64, error) {
	var err error
	var waitIdx uint64
	err = e.createTTLDir(30)
	if err != nil {
		log.Errorf("Create dir %s error %s", e.name, err.Error())
		return 0, err
	}

	if waitIdx, e.mykey, err = e.queueLock(ctx); err == nil {
		return waitIdx, nil
	} else {
		return 0, err
	}
}

func (e *EtcdRWLock) testLock(ctx context.Context) (bool, uint64, error) {
	if e.timeout != 0 {
		defer e.timeTrack(time.Now(), "testLock")
	}

	var curIdx uint64

	dir := path.Join(e.dir, e.name)
	resp, err := e.client.Get(ctx, dir, &client.GetOptions{Recursive: true, Sort: true})
	if err != nil {
		return false, 0, err
	}

	curIdx = resp.Index

	for ii, node := range resp.Node.Nodes {
		var state LockState
		if err := json.Unmarshal([]byte(node.Value), &state); err != nil {
			log.Errorf("RWLock %s Unmarshal error", err.Error())
			return false, curIdx, err
		}

		if e.lockType == LockTypeWrite && ii == 0 {
			if state.LockType != LockTypes[LockTypeWrite] ||
				e.mykey != node.Key {
				return false, curIdx, nil
			}
			if err := e.refreshTTL(ctx); err != nil {
				return false, curIdx, err
			}
			return true, curIdx, nil
		}

		if state.LockType != LockTypes[LockTypeRead] {
			return false, curIdx, nil
		}

		if e.mykey == node.Key {
			if err := e.refreshTTL(ctx); err != nil {
				return false, curIdx, err
			}
			return true, curIdx, nil
		}
	}

	return false, curIdx, ErrLostReq

}

func (e *EtcdRWLock) refreshTTL(ctx context.Context) error {
	_, err := e.client.Set(ctx, e.mykey, "", &client.SetOptions{PrevExist: client.PrevExist, TTL: 10 * time.Second, Refresh: true})
	return err
}

func (e *EtcdRWLock) queueLock(ctx context.Context) (uint64, string, error) {
	if e.timeout != 0 {
		defer e.timeTrack(time.Now(), "queueLock")
	}

	dir := path.Join(e.dir, e.name)
	if resp, err := e.client.CreateInOrder(ctx, dir, e.content, nil); err != nil {
		return 0, "", err
	} else {
		return resp.Index, resp.Node.Key, nil
	}
}

func (e *EtcdRWLock) timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	e.mtx.Lock()
	e.lefttime -= elapsed
	e.mtx.Unlock()
}

func (e *EtcdRWLock) createTTLDir(dirTTL uint64) error {
	if e.timeout != 0 {
		defer e.timeTrack(time.Now(), "queueLock")
	}

	var err error
	dir := path.Join(e.dir, e.name)
	op := client.SetOptions{TTL: time.Duration(dirTTL) * time.Second, Dir: true}
	_, err = e.client.Set(context.Background(), dir, "", &op)
	if err == nil {
		return nil
	}

	if cerr, ok := err.(client.Error); ok {
		if cerr.Code == client.ErrorCodeNotFile {
			log.Debugf("RWLock dir %s already exist; refresh it", dir)
			upop := client.SetOptions{TTL: time.Duration(dirTTL) * time.Second, Dir: true, PrevExist: client.PrevExist, Refresh: true}
			_, err = e.client.Set(context.Background(), dir, "", &upop)
		}
	}

	return err
}

func (e *EtcdRWLock) Lock(timeout time.Duration) error {
	if err := e.acquire(context.Background(), timeout); err != nil {
		return err
	}
	return e.Wait()
}

func (e *EtcdRWLock) Unlock() error {
	e.mtx.Lock()
	if !e.holding {
		e.mtx.Unlock()
		return ErrNotHold
	}
	e.mtx.Unlock()

	_, err := e.client.Delete(context.Background(), e.mykey, nil)
	if err == nil {
		return nil
	}

	if cerr, ok := err.(client.Error); ok {
		if cerr.Code == client.ErrorCodeKeyNotFound {
			log.Errorf("RWLock Key Not Found %s; Maybe expired", e.mykey)
			return nil
		}
	}

	return err
}

func (e *EtcdRWLock) EventsChan() <-chan LockEventType {
	return e.eventsCh
}

func (e *EtcdRWLock) Cancel() {
	e.mtx.Lock()
	if e.start {
		log.Debugf("Cancel lock %s routine", e.name)
		e.stopFunc()
	}
	e.mtx.Unlock()
}

func (e *EtcdRWLock) Wait() error {
	err := ErrDefault
	select {
	case e := <-e.eventsCh:
		if e == LockAcq {
			return nil
		} else if e == LockTimeout {
			return ErrTimeout
		} else if e == LockCancel {
			return ErrCanceled
		} else if e == LockCliError {
			return ErrCliError
		} else if e == LockError {
			return ErrUnkown
		}
	}
	return err
}
