package credis

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	PROTOCOL = "tcp" //connection protocol
)

var (
	MaxIdle     int           = 100
	MaxActive   int           = 200
	IdleTimeout time.Duration = time.Duration(28 * time.Second)
)

func NewPool(server string, password string, IdleTimeout time.Duration, MaxIdle, MaxActive int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     MaxIdle,
		MaxActive:   MaxActive,
		IdleTimeout: IdleTimeout,
		Dial: func() (redis.Conn, error) {
			options := redis.DialPassword(password)
			c, err := redis.Dial(PROTOCOL, server, options)
			if err != nil {
				return nil, err
			}
			return c, err

		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Wait: true,
	}
}

type DaoCfg struct {
	Addr     string
	Password string
}

type Dao struct {
	cfg       *DaoCfg
	RedisPool *redis.Pool
}

func (d *Dao) Close() {
	d.RedisPool.Close()
}

func New(addr string, passwd string) *Dao {
	pool := NewPool(addr, passwd, IdleTimeout, MaxIdle, MaxActive)
	var cfg DaoCfg
	cfg.Addr = addr
	cfg.Password = passwd
	d := &Dao{cfg: &cfg, RedisPool: pool}
	return d
}

func (d *Dao) DelKey(id string) error {
	conn := d.RedisPool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", id)
	return err
}

func (d *Dao) SetHKey(id string, hkey string, val []byte, expire int64) error {
	conn := d.RedisPool.Get()
	defer conn.Close()
	_, err := conn.Do("HSET", id, hkey, val)
	if err == nil && expire > 0 {
		conn.Do("EXPIRE", id, expire)
	}
	return err
}

func (d *Dao) SetKey(id string, val []byte, expire int64) error {
	conn := d.RedisPool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", id, val)
	if err == nil && expire > 0 {
		conn.Do("EXPIRE", id, expire)
	}
	return err
}

func (d *Dao) GetHKey(id string, hkey string) ([]byte, error) {
	conn := d.RedisPool.Get()
	defer conn.Close()
	r, err := conn.Do("HGET", id, hkey)
	return redis.Bytes(r, err)
}

func (d *Dao) GetKey(id string) ([]byte, error) {
	conn := d.RedisPool.Get()
	defer conn.Close()
	r, err := conn.Do("GET", id)
	return redis.Bytes(r, err)
}

func (d *Dao) DelHKey(id string, hkey string) error {
	conn := d.RedisPool.Get()
	defer conn.Close()
	_, err := conn.Do("HDEL", id, hkey)
	return err
}

func (d *Dao) GetAllHKey(id string) ([][]byte, error) {
	conn := d.RedisPool.Get()
	defer conn.Close()
	r, err := conn.Do("HGETALL", id)
	return redis.ByteSlices(r, err)
}

func (d *Dao) SetKeyExpire(id string, second int) {
	conn := d.RedisPool.Get()
	defer conn.Close()
	conn.Do("EXPIRE", id, second)
}

func (d *Dao) GetConn() redis.Conn {
	conn := d.RedisPool.Get()
	return conn
}
