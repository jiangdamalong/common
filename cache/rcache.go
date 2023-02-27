package cache

import (
	"errors"
	"strconv"

	"github.com/garyburd/redigo/redis"

	"github.com/jiangdamalong/common/cache/credis"
)

type RHCache struct {
	dao         *credis.Dao
	redisExpire int64

	unmarshalFunc UnmarshalFunc
}

type RHCacheGroup struct {
	cache     []RHCache
	groupSize int
}

func (cg *RHCacheGroup) Init(cfg string, umf UnmarshalFunc) error {
	redisCfg, err := LoadCfg(cfg)
	if err != nil {
		return err
	}
	cg.groupSize = redisCfg.RedisCount
	cg.cache = make([]RHCache, cg.groupSize)
	for i := 0; i < cg.groupSize; i++ {
		cg.cache[i].Init(redisCfg.Items[i].Ip+":"+strconv.FormatUint(uint64(redisCfg.Items[i].Port), 10), redisCfg.Items[i].Passwd, umf)
		cg.cache[i].redisExpire = redisCfg.RedisExpire
	}
	return nil
}

func (cg *RHCacheGroup) RHCache(id uint64) *RHCache {
	return &(cg.cache[id%uint64(cg.groupSize)])
}

func (c *RHCache) Init(redisAddr string, passwd string, umf UnmarshalFunc) {
	c.dao = credis.New(redisAddr, passwd)
	c.unmarshalFunc = umf
}

func (c *RHCache) Delete(key string, hkey string) error {
	return c.delete(key, hkey)
}

func (c *RHCache) delete(key string, hkey string) error {
	err := c.dao.DelHKey(key, hkey)
	//fmt.Printf("err %+v\n", err)
	return err
}

func (c *RHCache) Set(key string, hkey string, tb CacheNodeIf) error {
	return c.set(key, hkey, tb)
}

func (c *RHCache) set(key string, hkey string, tb CacheNodeIf) error {
	tr, _ := tb.(RedisNode)
	if tr != nil {
		buf := tr.Marshal()
		err := c.dao.SetHKey(key, hkey, buf, c.redisExpire)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("not vaild RedisNode")
}

func (c *RHCache) Add(key string, hkey string, tb CacheNodeIf) error {
	return c.add(key, hkey, tb)
}

func (c *RHCache) add(key string, hkey string, tb CacheNodeIf) error {
	tr, _ := tb.(RedisNode)
	if tr != nil {
		buf := tr.Marshal()
		err := c.dao.SetHKey(key, hkey, buf, c.redisExpire)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *RHCache) GetConn() redis.Conn {
	return c.dao.GetConn()
}
