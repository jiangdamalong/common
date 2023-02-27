package cache

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/jiangdamalong/common/cache/credis"
	"github.com/jiangdamalong/common/log"
)

type UnmarshalFunc func(string, []byte) CacheNodeIf
type LoadValueFunc func(string) CacheNodeIf

const (
	REDIS_EXPIRE       = 6 * 30 * 60
	CACHE_EXPIRE       = 30 * 60
	EMPTY_CACHE_EXPIRE = 30
)

type RedisCfgItem struct {
	Ip     string `json:"Ip"`
	Port   uint32 `json:"Port"`
	Passwd string `json:"Passwd"`
}

type RedisConfig struct {
	RedisCount  int            `json:"TotalRedisConfigCount"`
	Items       []RedisCfgItem `json:"RedisConfig"`
	RedisExpire int64          `json:"RedisExpire"`
	MemExpire   int64          `json:"MemExpire"`
	EmptyExpire int64          `json:"EmptyExpire"`
	ClearHour   int            `json:"ClearHour"`
	LruSize     int            `json:"LruSize"`
}

func LoadCfg(cfg string) (*RedisConfig, error) {
	buf, err := ioutil.ReadFile(cfg)
	if err != nil {
		return nil, err
	}
	var redisCfg = RedisConfig{RedisExpire: REDIS_EXPIRE, MemExpire: CACHE_EXPIRE, EmptyExpire: EMPTY_CACHE_EXPIRE}
	if err = json.Unmarshal(buf, &redisCfg); err != nil {
		return nil, err
	}

	if len(redisCfg.Items) == 0 {
		var jsonMap = make(map[string]interface{})
		json.Unmarshal(buf, &jsonMap)
		for i := 0; i < redisCfg.RedisCount; i++ {
			var k = "RedisConfig" + strconv.FormatInt(int64(i), 10)
			v, ok := jsonMap[k]
			if !ok {
				return nil, errors.New("credis old config invalid")
			}
			itemBuf, err := json.Marshal(v)
			if err != nil {
				return nil, errors.New("credis old config invalid")
			}
			var itemCfg RedisCfgItem
			if err = json.Unmarshal(itemBuf, &itemCfg); err != nil {
				return nil, errors.New("credis old config invalid")
			}
			redisCfg.Items = append(redisCfg.Items, itemCfg)
		}
	}

	if redisCfg.RedisCount != len(redisCfg.Items) || redisCfg.RedisCount <= 0 {
		return nil, errors.New("credis config invalid")
	}

	return &redisCfg, nil
}

type valContain struct {
	val     CacheNodeIf
	keyItem *list.Element
}

func (vc *valContain) getExpire() int64 {
	return vc.keyItem.Value.(*keyContain).expire
}

func (vc *valContain) setExpire(expire int64) {
	vc.keyItem.Value.(*keyContain).expire = expire
}

type keyContain struct {
	key    string
	expire int64
}

type SyncList struct {
	sync.RWMutex
	list.List
}

type Cache struct {
	sync.RWMutex
	items         map[string]*valContain
	dao           *credis.Dao
	keyList       SyncList
	unmarshalFunc UnmarshalFunc
	redisExpire   int64
	memExpire     int64
	emptyExpire   int64
	lruSize       int
	loadFunc      LoadValueFunc
}

type CacheGroup struct {
	cache     []Cache
	groupSize int
}

func (cg *CacheGroup) Init(cfg string, umf UnmarshalFunc, loadFunc LoadValueFunc) error {
	redisCfg, err := LoadCfg(cfg)
	if err != nil {
		return err
	}
	cg.groupSize = redisCfg.RedisCount
	cg.cache = make([]Cache, cg.groupSize)
	for i := 0; i < cg.groupSize; i++ {
		cg.cache[i].items = make(map[string]*valContain)
		cg.cache[i].Init(redisCfg.Items[i].Ip+":"+strconv.FormatUint(uint64(redisCfg.Items[i].Port), 10), redisCfg.Items[i].Passwd, umf)
		cg.cache[i].emptyExpire = redisCfg.EmptyExpire
		cg.cache[i].memExpire = redisCfg.MemExpire
		cg.cache[i].redisExpire = redisCfg.RedisExpire
		cg.cache[i].unmarshalFunc = umf
		cg.cache[i].loadFunc = loadFunc
	}
	return nil
}

func New(groupSize int) *CacheGroup {
	var cg CacheGroup
	cg.groupSize = groupSize
	cg.cache = make([]Cache, groupSize)
	for i := 0; i < groupSize; i++ {
		cg.cache[i].memExpire = CACHE_EXPIRE
		cg.cache[i].items = make(map[string]*valContain)
	}
	return &cg
}

func (cg *CacheGroup) SetLruSize(lruSize int) {
	for i := 0; i < cg.groupSize; i++ {
		cg.cache[i].lruSize = lruSize
	}
}

func (cg *CacheGroup) GetLen() int {
	var slen = 0
	for i := 0; i < cg.groupSize; i++ {
		slen += cg.cache[i].keyList.Len()
	}
	return slen
}

func (l *SyncList) PushBack(val interface{}) *list.Element {
	l.Lock()
	defer l.Unlock()
	return l.List.PushBack(val)
}

func (l *SyncList) PushFront(val interface{}) *list.Element {
	l.Lock()
	defer l.Unlock()
	return l.List.PushFront(val)
}

func (l *SyncList) Remove(e *list.Element) {
	l.Lock()
	defer l.Unlock()
	l.List.Remove(e)
}

func (l *SyncList) Front() *list.Element {
	l.Lock()
	defer l.Unlock()
	return l.List.Front()
}

func (l *SyncList) Next(e *list.Element) *list.Element {
	l.Lock()
	defer l.Unlock()
	return e.Next()
}

func (l *SyncList) Back() *list.Element {
	l.Lock()
	defer l.Unlock()
	return l.List.Back()
}

func (l *SyncList) Prev(e *list.Element) *list.Element {
	l.Lock()
	defer l.Unlock()
	return e.Prev()
}

func (l *SyncList) MoveToFront(e *list.Element) {
	l.Lock()
	defer l.Unlock()
	l.List.MoveToFront(e)
}

func (l *SyncList) MoveToBack(e *list.Element) {
	l.Lock()
	defer l.Unlock()
	l.List.MoveToBack(e)
}

var errExist = errors.New("node exist")

func (c *CacheGroup) getBlock(key string) *Cache {
	if c.groupSize == 1 {
		return &(c.cache[0])
	}
	base := crc32.ChecksumIEEE([]byte(key))
	return &(c.cache[base%uint32(c.groupSize)])
}

func (c *CacheGroup) Cache(id uint64) *Cache {
	return &(c.cache[id%uint64(c.groupSize)])
}

func (c *CacheGroup) HCache(id uint64) *Cache {
	return &(c.cache[id%uint64(c.groupSize)])
}

func (c *Cache) Init(redisAddr string, passwd string, umf UnmarshalFunc) {
	c.dao = credis.New(redisAddr, passwd)
	c.items = make(map[string]*valContain)
	c.unmarshalFunc = umf
	go c.timerHandler()
}

func (c *CacheGroup) Get(key string) CacheNodeIf {
	return c.getBlock(key).Get(key)
}

func (c *Cache) Read(key string) (CacheNodeIf, error) {
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v spend time %+v[ms] value %+v", key, spend/1000/1000)
		}
	}()
	v := c.Get(key)
	if v != nil {
		cv, _ := v.(CopyableNode)
		if cv == nil {
			return nil, errors.New("un copyable node " + reflect.TypeOf(v).PkgPath() + "." + reflect.TypeOf(v).Name())
		}
		return cv.CopyNode()
	}
	return nil, nil
}

func (c *Cache) Get(key string) CacheNodeIf {
	c.Lock()
	defer c.Unlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v spend time %+v[ms]", key, spend/1000/1000)
		}
	}()

	var v CacheNodeIf
	v = nil

	if con, ok := c.items[key]; ok {
		if con.getExpire() < time.Now().Unix() {
			v = nil
		} else {
			if con.val != nil {
				con.setExpire(c.memExpire + time.Now().Unix())
				c.keyList.MoveToFront(con.keyItem)
			}
			return con.val
		}
	}
	if v == nil && c.dao != nil {
		vb, err := c.dao.GetKey(key)
		if err != nil {
			v = nil
		} else {
			v = c.unmarshalFunc(key, vb)
			if v == nil {
				return nil
			}
			var con valContain
			var ckey keyContain
			ckey.key = key
			con.val = v
			con.keyItem = c.keyList.PushFront(&ckey)
			con.setExpire(c.memExpire + time.Now().Unix())
			c.items[key] = &con
		}
	}

	if v == nil && c.loadFunc != nil {
		//这里调了外部函数，需要先释放锁
		c.Unlock()
		v = c.loadFunc(key)
		c.Lock()
		//避免重入
		if con, ok := c.items[key]; ok {
			return con.val
		}
		if v != nil && !reflect.ValueOf(v).IsNil() {
			c.set(key, v)
		}
	}

	//需要做空值缓存，应对空值攻击
	if v == nil && c.emptyExpire > 0 {
		var con valContain
		var ckey keyContain
		ckey.key = key
		con.val = v
		//空值放最后，lru cache不足时优先淘汰
		con.keyItem = c.keyList.PushBack(&ckey)
		con.setExpire(c.emptyExpire + time.Now().Unix())
		c.items[key] = &con
	}

	return v
}

func (c *CacheGroup) Set(key string, val CacheNodeIf) error {
	return c.getBlock(key).Set(key, val)
}

func (c *Cache) Set(key string, val CacheNodeIf) error {
	c.Lock()
	defer c.Unlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v spend time %+v[ms] val %+v", key, spend/1000/1000, val)
		}
	}()
	return c.set(key, val)
}

func (c *Cache) set(key string, val CacheNodeIf) error {
	if c.dao != nil {
		rval, _ := val.(RedisNode)
		if rval != nil {
			buf := rval.Marshal()
			if err := c.dao.SetKey(key, buf, c.redisExpire); err != nil {
				return err
			}
		}
	}

	if cont, ok := c.items[key]; ok {
		cont.val = val
		cont.setExpire(c.memExpire + time.Now().Unix())
		c.keyList.MoveToFront(cont.keyItem)
		return nil
	}
	var con valContain
	var ckey keyContain
	ckey.key = key
	con.val = val
	if val != nil {
		con.keyItem = c.keyList.PushFront(&ckey)
	} else {
		con.keyItem = c.keyList.PushBack(&ckey)
	}
	con.setExpire(c.memExpire + time.Now().Unix())
	c.items[key] = &con
	return nil
}

//外部Delete不直接执行删除，由定时器负责回收，减轻锁开销
func (c *CacheGroup) Delete(key string) {
	c.getBlock(key).Delete(key)
}

func (c *Cache) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v spend time %+v[ms]", key, spend/1000/1000)
		}
	}()

	if con, ok := c.items[key]; ok {
		con.setExpire(0)
		c.keyList.MoveToBack(con.keyItem)
	}
	if c.dao != nil {
		c.dao.DelKey(key)
	}
	return
}

func (c *CacheGroup) DeleteMem(key string) {
	c.getBlock(key).DeleteMem(key)
}

func (c *Cache) DeleteMem(key string) {
	c.Lock()
	defer c.Unlock()
	tb := time.Now()
	defer func() {
		if spend := time.Now().Sub(tb); spend > 200*time.Millisecond {
			log.Errorf("key %+v spend time %+v[ms]", key, spend/1000/1000)
		}
	}()

	if con, ok := c.items[key]; ok {
		con.setExpire(0)
		c.keyList.MoveToBack(con.keyItem)
	}
}

func (c *Cache) PrintKeys() {
	for e := c.keyList.Front(); e != nil; e = c.keyList.Next(e) {
		fmt.Printf("%+v\n", e.Value)
	}
}

//从key list中取出元素，检查是否过期
//从尾部向头部遍历，以支持lru cache
func (c *Cache) timerHandler() {
	e := c.keyList.Back()
	i := 0
	var lruToDel = 0
	for {
		i++
		for e == nil {
			time.Sleep(1 * time.Second)
			e = c.keyList.Back()
			tlen := c.keyList.Len()
			//有设置lrusize，并且当前len大于设置lruSize
			if tlen > 0 && c.lruSize > 0 && tlen > c.lruSize {
				lruToDel = tlen - c.lruSize
			}
		}
		if i%100 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		tn := time.Now().Unix()

		var needDel = false
		if e.Value.(*keyContain).expire < tn {
			needDel = true
		}

		if lruToDel > 0 {
			needDel = true
		}

		if needDel == true {
			c.Lock()
			delete(c.items, e.Value.(*keyContain).key)
			c.Unlock()
			e1 := e
			e = c.keyList.Prev(e)
			c.keyList.Remove(e1)
			if lruToDel > 0 {
				lruToDel--
			}
		} else {
			e = c.keyList.Prev(e)
		}
	}
}

func (c *Cache) GetConn() redis.Conn {
	return c.dao.GetConn()
}
