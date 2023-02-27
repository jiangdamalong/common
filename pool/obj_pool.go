package pool

import (
	"reflect"
	"sync"
)

type ObjContainer struct {
	iv   interface{}
	rv   *reflect.Value
	next *ObjContainer
}

type NewFunc func() interface{}

type NoCopy struct{}
type ObjPool struct {
	NoCopy
	lock sync.Mutex

	contains          []ObjContainer
	freeContainer     *ObjContainer
	freeObjsInContain *ObjContainer

	limFreeObjNum int
	freeObjNum    int

	fnew NewFunc
	seq  int
}

func (pool *ObjPool) initContain() {
	pool.contains = make([]ObjContainer, pool.limFreeObjNum)
	pool.freeContainer = &(pool.contains[0])
	for i := 0; i < pool.limFreeObjNum-1; i++ {
		pool.contains[i].next = &(pool.contains[i+1])
	}
	pool.contains[pool.limFreeObjNum-1].next = nil
}

func (pool *ObjPool) Init(fnew NewFunc, freeObjNum int) {
	if freeObjNum <= 0 {
		panic("freeObjNum invalid")
	}
	pool.fnew = fnew

	pool.limFreeObjNum = freeObjNum

	pool.freeContainer = nil
	pool.freeObjsInContain = nil

	pool.freeObjNum = 0
	pool.seq = 0
	pool.initContain()
}

func (pool *ObjPool) InitRv(vtype reflect.Type, freeObjNum int) {
	if freeObjNum <= 0 {
		panic("freeObjNum invalid")
	}
	f := func() interface{} {
		var obj reflect.Value
		argIsValue := false
		if (vtype).Kind() == reflect.Ptr {
			obj = reflect.New((vtype).Elem())
		} else {
			obj = reflect.New((vtype))
			argIsValue = true
		}
		if argIsValue {
			obj = obj.Elem()
		}
		return reflect.Indirect(obj).Addr().Interface()
	}
	pool.Init(f, freeObjNum)
}

func (pool *ObjPool) SetDefault(freeObjNum int) {
	if freeObjNum <= 0 {
		panic("freeObjNum invalid")
	}
	pool.lock.Lock()
	defer pool.lock.Unlock()
	pool.limFreeObjNum = freeObjNum

	pool.freeContainer = nil
	pool.freeObjsInContain = nil

	pool.freeObjNum = 0

	pool.seq = 0
	pool.initContain()
}

func (pool *ObjPool) Alloc() interface{} {
	var iv interface{}
	pool.lock.Lock()
	if pool.freeObjsInContain != nil {
		t1 := pool.freeObjsInContain
		pool.freeObjsInContain = pool.freeObjsInContain.next

		iv = t1.iv

		t1.next = pool.freeContainer
		pool.freeContainer = t1

		t1.iv = nil
		pool.lock.Unlock()
	} else {
		pool.lock.Unlock()
		iv = pool.fnew()
	}
	return iv
}

func (pool *ObjPool) Free(iv interface{}) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	pool.seq++
	if pool.freeObjNum < pool.limFreeObjNum && pool.seq%128 != 0 {
		pool.freeObjNum++
		var container *ObjContainer
		if pool.freeContainer != nil {
			container = pool.freeContainer
			pool.freeContainer = pool.freeContainer.next
		} else {
			container = new(ObjContainer)
		}
		container.iv = iv
		container.next = pool.freeObjsInContain
		pool.freeObjsInContain = container
	}
}

type PoolMaps struct {
	Pool   map[reflect.Type]*ObjPool
	rwlock sync.RWMutex
}

func (pm *PoolMaps) GetPool(objType reflect.Type, maxFreeNodeNum int) *ObjPool {
	pm.rwlock.RLock()
	pool, ok := pm.Pool[objType]
	pm.rwlock.RUnlock()
	if ok != true {
		pm.rwlock.Lock()
		pool = new(ObjPool)

		f := func() interface{} {
			var obj reflect.Value
			argIsValue := false
			if (objType).Kind() == reflect.Ptr {
				obj = reflect.New((objType).Elem())
			} else {
				obj = reflect.New(objType)
				argIsValue = true
			}
			if argIsValue {
				obj = obj.Elem()
			}
			return reflect.Indirect(obj).Addr().Interface()
		}

		pool.Init(f, maxFreeNodeNum)
		pm.Pool[objType] = pool
		pm.rwlock.Unlock()
	}
	return pool
}
