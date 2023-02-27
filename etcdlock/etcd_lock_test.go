package etcdlock

import (
	"context"
	"sync"
	"testing"
	"time"
)

func wait(chann <-chan LockEventType, t *testing.T) {
	select {
	case e := <-chann:
		if e == LockAcq {
			t.Log("Got lock")
			time.Sleep(2 * time.Second)
		} else if e == LockTimeout {
			t.Errorf("Timeout")
		} else if e == LockCancel {
			t.Errorf("Canceled")
		} else if e == LockCliError {
			t.Errorf("EtcdClient Error")
		} else if e == LockError {
			t.Errorf("Unknown Error")
		}
	}
}

func TestAsyncMutex(t *testing.T) {
	locker, _ := NewAsyncEtcdMutex([]string{"http://10.1.101.15:23790"}, "TestAsyncMutex", 689000, 5)
	locker.Acquire(context.Background(), 10*time.Second)
	wait(locker.EventsChan(), t)

	if err := locker.Unlock(); err != nil {
		t.Errorf("Unlock error")
		t.Errorf(err.Error())
		return
	}
}

func TestMutex(t *testing.T) {
	locker, _ := NewEtcdMutex([]string{"http://10.1.101.15:23790"}, "TestMutex", 689000, 5)
	if err := locker.Lock(10 * time.Second); err != nil {
		t.Errorf(err.Error())
		return
	}

	if err := locker.Unlock(); err != nil {
		t.Errorf(err.Error())
		return
	}
}

func TestAsyncRWLockWrite(t *testing.T) {
	rwlocker, _ := NewAsyncEtcdRWLock([]string{"http://10.1.101.15:23790"}, "TestAsyncRWLockWrite", 689002, 20, LockTypeWrite)
	rwlocker.Acquire(context.Background(), 10*time.Second)

	wait(rwlocker.EventsChan(), t)
	if err := rwlocker.Unlock(); err != nil {
		t.Errorf(err.Error())
		return
	}
}

func TestAsyncRWLockRead(t *testing.T) {
	rwlocker, _ := NewAsyncEtcdRWLock([]string{"http://10.1.101.15:23790"}, "TestAsyncRWLockWrite", 689002, 20, LockTypeRead)
	rwlocker.Acquire(context.Background(), 10*time.Second)

	wait(rwlocker.EventsChan(), t)
	if err := rwlocker.Unlock(); err != nil {
		t.Errorf(err.Error())
		return
	}
}

func TestRWLockWrite(t *testing.T) {
	rwlocker, _ := NewEtcdRWLock([]string{"http://10.1.101.15:23790"}, "TestRWLockWrite", 689002, 20, LockTypeWrite)
	if ret := rwlocker.Lock(10 * time.Second); ret == nil {
		t.Log("GOT IT")
	} else {
		t.Errorf(ret.Error())
		return
	}

	if err := rwlocker.Unlock(); err != nil {
		t.Errorf(err.Error())
		return
	}
}

func TestRWLockRead(t *testing.T) {
	rwlocker, _ := NewEtcdRWLock([]string{"http://10.1.101.15:23790"}, "TestRWLockRead", 689002, 20, LockTypeRead)
	if ret := rwlocker.Lock(10 * time.Second); ret == nil {
		t.Log("GOT IT")
	} else {
		t.Errorf(ret.Error())
		return
	}

	if err := rwlocker.Unlock(); err != nil {
		t.Errorf(err.Error())
		return
	}
}

func TestMutexAtomicAdd(t *testing.T) {
	var wg sync.WaitGroup
	i := 0
	for c := 0; c < 10; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			locker, _ := NewEtcdMutex([]string{"http://10.1.101.15:23790"}, "TestMutex", 689000, 5)
			if err := locker.Lock(0); err != nil {
				t.Errorf(err.Error())
				return
			}

			i++

			if err := locker.Unlock(); err != nil {
				t.Errorf(err.Error())
				return
			}
		}()
	}

	wg.Wait()

	if i != 10 {
		t.Errorf("Error")
	}

	t.Log(i)
}

func TestRWLockAtominAdd(t *testing.T) {
	var wg sync.WaitGroup
	i := 0
	for c := 0; c < 10; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rwlocker, _ := NewEtcdRWLock([]string{"http://10.1.101.15:23790"}, "TestRWLockWrite", 689002, 20, LockTypeWrite)
			if err := rwlocker.Lock(0); err != nil {
				t.Errorf(err.Error())
				return
			}

			i++

			if err := rwlocker.Unlock(); err != nil {
				t.Errorf(err.Error())
				return
			}
		}()
	}

	wg.Wait()

	if i != 10 {
		t.Errorf("Error")
	}

	t.Log(i)
}
