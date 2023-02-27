package cache

import (
	"container/list"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/jiangdamalong/common/cache/credis"
	"github.com/jiangdamalong/common/log"
)

const (
	TASK_RET_FAILED             = -1
	TASK_RET_CONTINUE           = 0
	TASK_RET_FINISH             = 1
	TASK_RET_CONSUMED           = 2
	TASK_RET_DELAY_CACHE_EXPIRE = 3
	TASK_RET_WRITE_BACK_STORE   = 4
)

const (
	NODE_WALK_FINISH   = 13000001
	NODE_WALK_CONTINUE = 0
)

type CacheNodeIf interface{}

// 可拷贝节点，用于read接口
type CopyableNode interface {
	CopyNode() (CacheNodeIf, error)
}

// 事件节点，用于HandleEvet
type EventNode interface {
	HandleEvent(en interface{}) (int, int)
}

// redis存储节点，用于redis存储，设置dao的情况下，会存储到redis中
type RedisNode interface {
	Marshal() []byte
	Unmarshal(val []byte) error
	IsValid() bool //是否有效，从redis中读取的数据，可能已经失效，读取出来，需要预先检查，仅hash cache有效
}

type HCacheNode struct {
	sync.Mutex
	hitems    map[string]CacheNodeIf
	keyIter   *list.Element
	dirtyKeys map[string]bool
}

type keyNode struct {
	key         string
	cacheExpire int64
	isMemExpire bool
}

type HCache struct {
	sync.RWMutex //读写锁
	dao          *credis.Dao
	items        map[string]*HCacheNode
	keyList      SyncList
	emptyExpire  int64
	memExpire    int64
	redisExpire  int64
	lruSize      int //lru存储节点数限制

	//每天凌晨n点，每6分钟清理一块内存
	clearMinute   int
	clearHour     int
	unmarshalFunc UnmarshalFunc
}

type HCacheGroup struct {
	cache     []HCache
	groupSize int
}

func (cg *HCacheGroup) Init(cfg string, umf UnmarshalFunc) error {
	redisCfg, err := LoadCfg(cfg)
	if err != nil {
		return err
	}
	if redisCfg.ClearHour == 0 {
		redisCfg.ClearHour = -1
	}
	cg.groupSize = redisCfg.RedisCount
	cg.cache = make([]HCache, cg.groupSize)
	for i := 0; i < cg.groupSize; i++ {
		cg.cache[i].Init(redisCfg.Items[i].Ip+":"+strconv.FormatUint(uint64(redisCfg.Items[i].Port), 10), redisCfg.Items[i].Passwd, umf)
		cg.cache[i].emptyExpire = redisCfg.EmptyExpire
		cg.cache[i].memExpire = redisCfg.MemExpire
		cg.cache[i].redisExpire = redisCfg.RedisExpire
		//每6分钟清理一块
		cg.cache[i].clearMinute = i % 10
		cg.cache[i].clearHour = redisCfg.ClearHour
		if redisCfg.LruSize > 0 {
			cg.cache[i].lruSize = redisCfg.LruSize
		}
	}

	return nil
}

// 内存cache，
// num:组数
// memExpire:超时时长
func (cg *HCacheGroup) InitMem(num int, memExpire int64) error {
	cg.groupSize = num
	cg.cache = make([]HCache, cg.groupSize)
	for i := 0; i < cg.groupSize; i++ {
		cg.cache[i].initMem(memExpire)
	}
	return nil
}

func (cg *HCacheGroup) SetLruSize(lruSize int) {
	for i := 0; i < cg.groupSize; i++ {
		cg.cache[i].lruSize = lruSize
	}
}

func (cg *HCacheGroup) GetLen() int {
	var slen = 0
	for i := 0; i < cg.groupSize; i++ {
		slen += cg.cache[i].keyList.Len()
		log.Infof("get cache index %+v len %+v", i, cg.cache[i].keyList.Len())
	}
	return slen
}

func (cg *HCacheGroup) HCache(id uint64) *HCache {
	return &(cg.cache[id%uint64(cg.groupSize)])
}

func (c *HCache) Init(redisAddr string, passwd string, umf UnmarshalFunc) {
	c.dao = credis.New(redisAddr, passwd)
	c.items = make(map[string]*HCacheNode)
	c.unmarshalFunc = umf
	go c.timerHandler()
}

func (c *HCache) initMem(expire int64) {
	c.dao = nil
	c.items = make(map[string]*HCacheNode)
	c.unmarshalFunc = nil
	c.memExpire = expire
	go c.timerHandler()
}

func (c *HCache) load(key string) (int, error) {
	t, ok := c.items[key]
	ct := 0
	now := time.Now().Unix()
	if ok && (t.keyIter.Value.(*keyNode).cacheExpire < now || t.keyIter.Value.(*keyNode).isMemExpire == true) {
		t = nil
		ok = false
	}
	if ok {
		//存在节点，load时，将keyList中的记录，放到最前面，有效期续期
		if len(t.hitems) > 0 {
			c.keyList.MoveToFront(t.keyIter)
			t.keyIter.Value.(*keyNode).cacheExpire = c.memExpire + time.Now().Unix()
		}
		return len(t.hitems), nil
	}
	//开读锁，上写锁，最后恢复
	c.RUnlock()
	c.Lock()
	defer func() {
		c.Unlock()
		c.RLock()
	}()

	if c.dao == nil {
		return 0, nil
	}
	//从redis取数据
	vals, err := c.dao.GetAllHKey(key)
	if err != nil || len(vals) == 0 {
		return 0, err
	}

	//重新检查是否有节点，防止重复进入
	t, ok = c.items[key]
	if ok && (t.keyIter.Value.(*keyNode).cacheExpire < now || t.keyIter.Value.(*keyNode).isMemExpire == true) {
		t = nil
		ok = false
	}
	if ok {
		return len(t.hitems), nil
	}

	var cn HCacheNode
	//cn.dirtyKeys = make(map[string]bool)
	if t != nil && len(t.dirtyKeys) > 0 {
		cn.dirtyKeys = t.dirtyKeys
	} else {
		cn.dirtyKeys = make(map[string]bool)
	}

	var tm = make(map[string]CacheNodeIf)
	for i := 0; i < len(vals)/2; i++ {
		//有脏数组的key
		if _, ok := cn.dirtyKeys[string(vals[i*2])]; ok == true {
			if v, ok := t.hitems[string(vals[i*2])]; ok == true {
				tm[string(vals[i*2])] = v
				continue
			} else {
				delete(cn.dirtyKeys, string(vals[i*2]))
			}
		}
		th := c.unmarshalFunc(string(vals[i*2]), vals[i*2+1])
		var tr RedisNode
		//fmt.Printf("get exist node %+v %+v\n", string(vals[i*2]), th)
		if th != nil {
			tr, _ = th.(RedisNode)
		}
		if tr != nil {
			/*ct++
			tm[string(vals[i*2])] = th*/
			if tr.IsValid() == false {
				c.delete(key, string(vals[i*2]), false)
				//fmt.Printf("get expire node %+v %+v\n", string(vals[i*2]), th)
			} else {
				tm[string(vals[i*2])] = th
				ct++
			}
		}
	}
	var kn keyNode
	kn.key = key
	kn.isMemExpire = false
	if len(vals) > 0 {
		kn.cacheExpire = c.memExpire + time.Now().Unix()
		cn.keyIter = c.keyList.PushFront(&kn)
	} else {
		//空值缓存放最后，lrucache不足时优先淘汰
		kn.cacheExpire = c.emptyExpire + time.Now().Unix()
		cn.keyIter = c.keyList.PushBack(&kn)
	}

	cn.hitems = tm
	c.items[key] = &cn
	return ct, nil
}

// 遍历访问所有hash节点
func (c *HCache) WalkNode(key string, f interface{}, v ...interface{}) error {
	c.RLock()
	defer c.RUnlock()
	cnt, err := c.load(key)
	if err != nil {
		return err
	}

	if cnt == 0 {
		return nil
	}

	t, ok := c.items[key]
	if !ok {
		return nil
	}

	t.Lock()
	defer t.Unlock()
	rf := reflect.ValueOf(f)
	rv := make([]reflect.Value, len(v)+2)
	for i, iv := range v {
		//rv = append(rv, reflect.ValueOf(iv))
		rv[i+2] = reflect.ValueOf(iv)
	}

	for key, item := range t.hitems {
		rv[0] = reflect.ValueOf(key)
		rv[1] = reflect.ValueOf(item)
		ret := rf.Call(rv)
		//第一个ret为int，等于NODE_WALK_FINISH，即认为结束
		if len(ret) > 0 {
			if ret[0].Kind() == reflect.Int {
				if ret[0].Int() == NODE_WALK_FINISH {
					break
				}
			}
		}
	}
	return nil
}

func (c *HCache) Delete(key string, hkey string) error {
	c.RLock()
	defer c.RUnlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v hkey %+v spend time %+v[ms]", key, hkey, spend/1000/1000)
		}
	}()

	c.load(key)
	return c.delete(key, hkey, false)
}

func (c *HCache) DeleteMem(key string) error {
	c.RLock()
	defer c.RUnlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v spend time %+v[ms] value %+v", key, spend/1000/1000)
		}
	}()
	t, ok := c.items[key]
	if ok {
		t.Lock()
		defer t.Unlock()
		t.keyIter.Value.(*keyNode).isMemExpire = true
		c.keyList.MoveToBack(t.keyIter)
	}
	return nil
}

func (c *HCache) delete(key string, hkey string, isLocked bool) error {
	t, ok := c.items[key]
	if ok {
		if !isLocked {
			t.Lock()
			defer t.Unlock()
		}
		delete(t.hitems, hkey)
		if len(t.hitems) == 0 {
			t.keyIter.Value.(*keyNode).cacheExpire = 0
			c.keyList.MoveToBack(t.keyIter)
		}
	}

	if c.dao == nil {
		return nil
	}
	err := c.dao.DelHKey(key, hkey)
	//fmt.Printf("err %+v\n", err)
	return err
}

func (c *HCache) Set(key string, hkey string, tb CacheNodeIf) error {
	c.RLock()
	defer c.RUnlock()
	begin := time.Now()
	defer func() {
		if spend := time.Now().Sub(begin); spend > 200*time.Millisecond {
			log.Errorf("key %+v hkey %+v spend time %+v[ms] value %+v", key, hkey, spend/1000/1000, tb)
		}
	}()
	//fmt.Printf("lock info key %+v %+v\n", key, c.RWMutex)
	_, err := c.load(key)
	if err != nil {
		return err
	}
	err = c.set(key, hkey, tb, false)
	return err
}

func (c *HCache) SetThrough(key string, hkey string, tb CacheNodeIf) error {
	begin := time.Now()
	defer func() {
		if spend := time.Now().Sub(begin); spend > 200*time.Millisecond {
			log.Errorf("key %+v hkey %+v spend time %+v[ms] value %+v", key, hkey, spend/1000/1000, tb)
		}
	}()

	var daoErr error = nil
	var tr RedisNode
	if c.dao != nil {
		tr, _ = tb.(RedisNode)
	}
	if c.dao != nil && tr != nil {
		buf := tr.Marshal()
		daoErr = c.dao.SetHKey(key, hkey, buf, c.redisExpire)
		if daoErr != nil {
			for i := 0; i < 3; i++ {
				if daoErr = c.dao.SetHKey(key, hkey, buf, c.redisExpire); daoErr == nil {
					break
				}
			}
		}
		if daoErr != nil {
			log.Errorf("set redis key+hkey error key:%s hkey:%s error:%+v", key, hkey, daoErr)
			return daoErr
		}
	}
	return c.DeleteMem(key)
}

func (c *HCache) set(key string, hkey string, tb CacheNodeIf, isLocked bool) error {
	/*ut, ok := c.items[key]
	if ok {
		ut.hitems[hkey] = tb
	}*/
	var daoErr error = nil
	var tr RedisNode
	if c.dao != nil {
		tr, _ = tb.(RedisNode)
	}
	if c.dao != nil && tr != nil {
		buf := tr.Marshal()
		daoErr = c.dao.SetHKey(key, hkey, buf, c.redisExpire)
		if daoErr != nil {
			for i := 0; i < 3; i++ {
				if daoErr = c.dao.SetHKey(key, hkey, buf, c.redisExpire); daoErr == nil {
					break
				}
			}
		}
		if daoErr != nil {
			log.Errorf("set redis key+hkey error key:%s hkey:%s error:%+v", key, hkey, daoErr)
		}
	}
	//有内存存储，需要刷内存
	ut, ok := c.items[key]
	if ok {
		if !isLocked {
			ut.Lock()
			defer ut.Unlock()
		}
		ut.hitems[hkey] = tb
		if daoErr != nil {
			ut.dirtyKeys[hkey] = true
		}
	} else {
		//如果不存在，则直接写回
		if c.dao != nil {
			return nil
		}
		c.RUnlock()
		c.Lock()
		defer func() {
			c.Unlock()
			c.RLock()
		}()
		t, ok := c.items[key]
		if !ok {
			t = new(HCacheNode)
			t.hitems = make(map[string]CacheNodeIf)
			var kn keyNode
			kn.key = key
			kn.cacheExpire = c.memExpire + time.Now().Unix()
			t.keyIter = c.keyList.PushFront(&kn)
			c.items[key] = t
		}
		t.hitems[hkey] = tb
	}
	return nil
}

func (c *HCache) flush(key string, hkey string, tb CacheNodeIf) error {
	var tr RedisNode
	tr, _ = tb.(RedisNode)
	if tr == nil {
		return nil
	}
	buf := tr.Marshal()
	err := c.dao.SetHKey(key, hkey, buf, c.redisExpire)
	if err != nil {
		log.Errorf("flush redis key+hkey error key:%s hkey:%s error:%+v", key, hkey, err)
	}
	return err
}

func (c *HCache) Get(key string, hkey string) (CacheNodeIf, error) {
	c.RLock()
	defer c.RUnlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v hkey %+v spend time %+v[ms]", key, hkey, spend/1000/1000)
		}
	}()
	_, err := c.load(key)
	if err != nil {
		return nil, err
	}
	return c.get(key, hkey)
}

func (c *HCache) PrintKeys() {
	for e := c.keyList.Front(); e != nil; e = c.keyList.Next(e) {
		fmt.Printf("%+v\n", e.Value)
	}
}

func (c *HCache) get(key string, hkey string) (CacheNodeIf, error) {
	t, ok := c.items[key]
	if !ok {
		return nil, nil
	}
	t.Lock()
	defer t.Unlock()
	hv, ok := t.hitems[hkey]
	if ok {
		tr, _ := hv.(RedisNode)
		if tr != nil {
			if tr.IsValid() == false {
				c.delete(key, hkey, true)
				return nil, nil
			}
		}
		return hv, nil
	}
	return nil, nil
}

func (c *HCache) Read(key string, hkey string) (CacheNodeIf, error) {
	c.RLock()
	defer c.RUnlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v hkey %+v spend time %+v[ms]", key, hkey, spend/1000/1000)
		}
	}()
	_, err := c.load(key)
	if err != nil {
		return nil, err
	}
	v, _ := c.get(key, hkey)
	if v == nil {
		return nil, nil
	}

	if cp, ok := v.(CopyableNode); !ok {
		return nil, errors.New("un copyable node " + reflect.TypeOf(v).PkgPath() + "." + reflect.TypeOf(v).Name())
	} else {
		return cp.CopyNode()
	}
}

func (c *HCache) Add(key string, hkey string, tb CacheNodeIf) error {
	c.RLock()
	defer c.RUnlock()
	begin := time.Now()
	defer func() {
		if spend := time.Now().Sub(begin); spend > 200*time.Millisecond {
			log.Errorf("key %+v hkey %+v spend time %+v[ms] value %+v", key, hkey, spend/1000/1000, tb)
		}
	}()
	_, err := c.load(key)
	if err != nil {
		return err
	}
	return c.add(key, hkey, tb)
}

func (c *HCache) add(key string, hkey string, tb CacheNodeIf) error {
	t, ok := c.items[key]
	if !ok {
		c.RUnlock()
		c.Lock()
		defer func() {
			c.Unlock()
			c.RLock()
		}()
		t, ok = c.items[key]
		if !ok {
			t = new(HCacheNode)
			t.hitems = make(map[string]CacheNodeIf)
			var kn keyNode
			kn.key = key
			kn.cacheExpire = c.memExpire + time.Now().Unix()
			t.keyIter = c.keyList.PushFront(&kn)
			c.items[key] = t
		}
	}

	t.Lock()
	defer t.Unlock()
	c.keyList.MoveToFront(t.keyIter)
	v, ok := t.hitems[hkey]
	if ok && v != nil {
		return errors.New("key used")
	}
	t.hitems[hkey] = tb
	if c.dao == nil {
		return nil
	}
	tr, _ := tb.(RedisNode)
	if tr == nil {
		return nil
	}

	buf := tr.Marshal()
	err := c.dao.SetHKey(key, hkey, buf, c.redisExpire)
	if err != nil {
		return err
	}
	return nil
}

// 针对uid名下所有任务，处理事件
func (c *HCache) HandleEvent(key string, hkey string, en interface{}) error {
	c.RLock()
	defer c.RUnlock()
	//fmt.Printf("key %s hkey %s\n", key, hkey)
	_, err := c.load(key)
	if err != nil {
		return err
	}
	t, ok := c.items[key]
	if !ok {
		return nil
	}

	t.Lock()
	defer t.Unlock()
	if hkey == "" {
		for hkey, v := range t.hitems {
			eNode, _ := v.(EventNode)
			if eNode == nil {
				continue
			}
			ret1, ret2 := eNode.HandleEvent(en)
			switch ret1 {
			case TASK_RET_WRITE_BACK_STORE:
				c.set(key, hkey, v, true)
			case TASK_RET_FAILED, TASK_RET_FINISH:
				c.delete(key, hkey, true)
			}
			if ret2 == TASK_RET_CONSUMED {
				break
			}
		}
	} else {
		node, ok := t.hitems[hkey]
		if !ok {
			return nil
		}
		eNode, _ := node.(EventNode)
		if eNode == nil {
			return errors.New("not event node")
		}
		ret1, _ := eNode.HandleEvent(en)
		switch ret1 {
		case TASK_RET_WRITE_BACK_STORE:
			c.set(key, hkey, node, true)
		case TASK_RET_FAILED, TASK_RET_FINISH:
			c.delete(key, hkey, true)
		}
	}
	return nil
}

// 从尾部向头部遍历，以支持lru cache
func (c *HCache) timerHandler() {
	e := c.keyList.Back()
	i := 0
	var lruToDel = 0
	var clearDay = 0
	for {
		i++
		now := time.Now()
		if now.Day() != clearDay && now.Hour() == c.clearHour && now.Minute()/6 == c.clearMinute {
			c.Lock()
			c.keyList.Init()
			c.items = make(map[string]*HCacheNode)
			c.Unlock()
			clearDay = now.Day()
			e = nil
			log.Info("clear hcache here ", c.clearMinute)
		}

		for e == nil {
			time.Sleep(20 * time.Millisecond)
			e = c.keyList.Back()
			tlen := c.keyList.Len()

			now = time.Now()
			if now.Day() != clearDay && now.Hour() == c.clearHour && now.Minute()/6 == c.clearMinute {
				c.Lock()
				c.keyList.Init()
				c.items = make(map[string]*HCacheNode)
				c.Unlock()
				clearDay = now.Day()
				e = nil
				log.Info("clear hcache here ", c.clearMinute)
			}

			//有设置lrusize，并且当前len大于设置lruSize
			if tlen > 0 && c.lruSize > 0 && tlen > c.lruSize {
				lruToDel = tlen - c.lruSize
			}
		}

		if i%100 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
		tn := time.Now().Unix()
		c.Lock()
		key := e.Value.(*keyNode).key
		n, ok := c.items[key]
		if ok && len(n.hitems) > 0 {
			n.Lock()
			for hk, hv := range n.hitems {
				if redisV, rok := hv.(RedisNode); rok && redisV != nil {
					if redisV.IsValid() == false {
						c.delete(key, hk, true)
					}
				}
			}
			n.Unlock()
		}
		//有c.dao，需要写回
		if c.dao != nil {
			if ok {
				n.Lock()
				for k, _ := range n.dirtyKeys {
					if v, ok := n.hitems[k]; ok == true {
						err := c.flush(key, k, v)
						if err == nil {
							delete(n.dirtyKeys, k)
						}
					} else {
						delete(n.dirtyKeys, k)
					}
				}
				n.Unlock()
			}
		}

		var needDel = false
		if e.Value.(*keyNode).cacheExpire < tn || e.Value.(*keyNode).isMemExpire == true {
			needDel = true
		}

		if lruToDel > 0 {
			needDel = true
		}

		if needDel == true {
			delete(c.items, key)
			e1 := e
			e = c.keyList.Prev(e)
			c.keyList.Remove(e1)
			if lruToDel > 0 {
				lruToDel--
			}
		} else {
			//e = c.keyList.Prev(e)
			e = nil
		}
		c.Unlock()
	}
}

func (c *HCache) GetConn() redis.Conn {
	return c.dao.GetConn()
}
