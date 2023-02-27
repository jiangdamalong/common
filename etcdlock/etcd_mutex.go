package etcdlock

import (
	"context"
	"errors"
	"fmt"
	"github.com/coreos/etcd/client"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/jiangdamalong/common/log"
)

type LockEventType int

const (
	LockAcq LockEventType = iota
	LockError
	LockCliError
	LockTimeout
	LockCancel
)

type AsyncEtcdLockInterface interface {
	Acquire(ctx context.Context, timeout time.Duration) error
	Cancel()
	EventsChan() <-chan LockEventType
	Wait() error
	Unlock() error
}

type EtcdLockInterface interface {
	Lock(timeout time.Duration) error
	Unlock() error
}

type EtcdTryLockInterface interface {
	Trylock() bool
	Unlock() error
}

type ClientAPI client.KeysAPI

type EtcdMutex struct {
	mtx         sync.Mutex
	client      ClientAPI
	dir         string
	name        string
	svrId       int
	ttl         uint64
	content     string
	watchStopCh chan bool
	stopFunc    context.CancelFunc
	eventsCh    chan LockEventType
	stoppedCh   chan bool
	holding     bool
	start       bool
}

func NewEtcdMutex(endpoints []string, name string, server_id int, ttl uint64) (EtcdLockInterface, error) {
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

	return &EtcdMutex{client: clientAPI, name: name, svrId: server_id, ttl: ttl,
		dir:         "/mutex",
		content:     "",
		watchStopCh: make(chan bool, 1),
		stopFunc:    nil,
		eventsCh:    make(chan LockEventType, 1),
		stoppedCh:   make(chan bool, 1),
		holding:     false,
		start:       false}, nil
}

func NewEtcdTryMutex(endpoints []string, name string, server_id int, ttl uint64) (EtcdTryLockInterface, error) {
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

	return &EtcdMutex{client: clientAPI, name: name, svrId: server_id, ttl: ttl,
		dir:         "/mutex",
		content:     "",
		watchStopCh: make(chan bool, 1),
		stopFunc:    nil,
		eventsCh:    make(chan LockEventType, 1),
		stoppedCh:   make(chan bool, 1),
		holding:     false,
		start:       false}, nil
}

func NewAsyncEtcdMutex(endpoints []string, name string, server_id int, ttl uint64) (AsyncEtcdLockInterface, error) {
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

	return &EtcdMutex{client: clientAPI, name: name, svrId: server_id, ttl: ttl,
		dir:         "/mutex",
		content:     "",
		watchStopCh: make(chan bool, 1),
		stopFunc:    nil,
		eventsCh:    make(chan LockEventType, 1),
		stoppedCh:   make(chan bool, 1),
		holding:     false,
		start:       false}, nil
}

func (e *EtcdMutex) Acquire(ctx context.Context, timeout time.Duration) error {
	log.Debugf("Acquire Lock routine Lockname :%s, Timeout :%s", e.name, timeout.String())

	e.mtx.Lock()
	if e.start {
		e.mtx.Unlock()
		log.Errorf("Lock routine exist")
		return fmt.Errorf("Lock routine exist")
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

func (e *EtcdMutex) acquire(ctx context.Context, timeout time.Duration) (ret error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1<<16)
			stackSize := runtime.Stack(buf, true)
			errMsg := fmt.Sprintf("Recovered from panic: %s", buf[:stackSize])
			log.Errorf("Lock routine panic : %s", errMsg)
			ret = errors.New(errMsg)
		}
	}()

	e.content = strconv.FormatInt(int64(e.svrId), 10)

	var resp *client.Response
	err := fmt.Errorf("Default error")
	lt := timeout

	k := path.Join(e.dir, e.name)
	v := e.content
	setop := client.SetOptions{TTL: time.Duration(e.ttl) * time.Second, PrevExist: client.PrevNoExist}

	var curctx context.Context

	chklt := func() bool {
		if timeout != 0 {
			if lt <= 5*time.Millisecond {
				e.eventsCh <- LockTimeout
				return false
			}
			curctx, _ = context.WithTimeout(ctx, lt)
		} else {
			curctx = ctx
		}

		return true
	}

	for {
		var wait_index uint64

		if !chklt() {
			log.Debugf("%s timeout", e.name)
			break
		}

		start := time.Now()
		resp, err = e.client.Set(ctx, k, v, &setop)
		elapsed := time.Since(start)
		log.Debugf("Mutex Set %s resp: %s", e.name, resp)
		lt -= elapsed

		if err == nil {
			log.Debugf("Mutex %s lock SUCCESS", e.name)
			e.mtx.Lock()
			e.holding = true
			e.mtx.Unlock()
			e.eventsCh <- LockAcq
			break
		} else {
			if cerr, ok := err.(client.Error); ok && cerr.Code == client.ErrorCodeNodeExist {
				log.Debugf("Mutex Set %s failed: %s", e.name, cerr.Error())
				wait_index = cerr.Index
			} else {
				log.Errorf("Mutex error %s: %s", e.name, err.Error())
				e.eventsCh <- errToEvent(err)
				break
			}

		}

		if !chklt() {
			log.Debugf("%s timeout", e.name)
			break
		}

		watcher := e.client.Watcher(k, &client.WatcherOptions{AfterIndex: wait_index})
		log.Debugf("Watch index after: %d", wait_index)
		start = time.Now()
		resp, err = watcher.Next(curctx)
		elapsed = time.Since(start)
		log.Debugf("Mutex Watch %s resp: %s", e.name, resp)
		lt -= elapsed

		if err == nil {
			continue
		} else {
			log.Errorf("Mutex error %s: %s", e.name, err.Error())
			e.eventsCh <- errToEvent(err)
			break
		}
	}

	return nil
}

func (e *EtcdMutex) Cancel() {
	e.mtx.Lock()
	if e.start {
		log.Debugf("Cancel lock %s routine", e.name)
		e.stopFunc()
	}
	e.mtx.Unlock()
}

func (e *EtcdMutex) EventsChan() <-chan LockEventType {
	return e.eventsCh
}

func (e *EtcdMutex) Wait() error {
	err := fmt.Errorf("Default error")
	select {
	case e := <-e.eventsCh:
		if e == LockAcq {
			return nil
		} else if e == LockTimeout {
			return fmt.Errorf("Timeout")
		} else if e == LockCancel {
			return fmt.Errorf("Canceled")
		} else if e == LockCliError {
			return fmt.Errorf("EtcdClient Error")
		} else if e == LockError {
			return fmt.Errorf("Unknown Error")
		}
	}
	return err
}

func (e *EtcdMutex) Trylock() bool {
	k := path.Join(e.dir, e.name)
	v := strconv.FormatInt(int64(e.svrId), 10)
	e.content = v
	setop := client.SetOptions{TTL: time.Duration(e.ttl) * time.Second, PrevExist: client.PrevNoExist}
	if resp, err := e.client.Set(context.Background(), k, v, &setop); err == nil {
		log.Debugf("Mutex Trylock %s SUCCESS: Resp: %s", e.name, resp)
		e.mtx.Lock()
		e.holding = true
		e.mtx.Unlock()
		return true
	} else {
		log.Debugf("Mutex Trylock %s FAILED: Resp: %s", e.name, resp)
		return false
	}
}

func (e *EtcdMutex) Lock(timeout time.Duration) error {
	if err := e.acquire(context.Background(), timeout); err != nil {
		return err
	}
	return e.Wait()
}

func (e *EtcdMutex) Unlock() error {
	e.mtx.Lock()
	if !e.holding {
		e.mtx.Unlock()
		return fmt.Errorf("No Holding")
	}
	e.mtx.Unlock()

	var cur string
	k := path.Join(e.dir, e.name)
	if resp, err := e.client.Get(context.Background(), k, nil); err != nil {
		return err
	} else {
		cur = resp.Node.Value
	}

	if cur != e.content {
		log.Errorf("Content diff:[My content :%s]; [Etcd content :%s]", e.content, cur)
		return fmt.Errorf("Not holding")
	}

	_, err := e.client.Delete(context.Background(), k, nil)
	if err == nil {
		return nil
	}

	if cerr, ok := err.(client.Error); ok {
		if cerr.Code == client.ErrorCodeKeyNotFound {
			log.Errorf("MutexKey Not Found %s; Maybe expired", e.name)
			return nil
		}
	}

	return err
}
