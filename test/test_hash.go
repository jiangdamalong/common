package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jiangdamalong/common/cache"
)

type Node struct {
	T string
	V string
}

func (n *Node) Marshal() []byte {
	return []byte(n.T + ":" + n.V)
}

func (n *Node) Unmarshal(val []byte) error {
	astr := strings.Split(string(val), ":")
	if len(astr) > 1 {
		n.T = astr[0]
		n.V = astr[1]
	} else {
		fmt.Printf("err string %+v\n", string(val))
	}
	return nil
}

func (n *Node) HandleEvent(en interface{}) (int, int) {
	/*if n.V == "1"*/ {
		n.V = "2222"
		return cache.TASK_RET_WRITE_BACK_STORE, 0
	}
	return 0, 0
}

func (n *Node) IsValid() bool {
	return true
}

func (n *Node) CopyNode() (cache.CacheNodeIf, error) {
	var cn Node
	cn = *n
	return &cn, nil
}

func ParseProto(hkey string, val []byte) cache.CacheNodeIf {
	var n Node
	n.Unmarshal(val)
	return &n
}

func main() {
	var hashCache cache.CacheGroup
	hashCache.Init("./food.json", ParseProto, nil)
	hashCache.SetLruSize(10)
	for i := 0; i < 12; i++ {
		var n Node
		n.T = strconv.FormatInt(int64(i), 10)
		n.V = strconv.FormatInt(int64(i), 10)
		hashCache.Cache(uint64(i)).Set(n.T, &n)
	}

	hashCache.Cache(uint64(13)).Set("13", nil)
	hashCache.Cache(1).PrintKeys()
	time.Sleep(time.Second)
	hashCache.Cache(1).Get("1")
	hashCache.Cache(1).PrintKeys()
	//hashCache.Cache(1).Delete("1")
	//hashCache.Cache(1).DeleteMem("1")
	hashCache.Cache(1).PrintKeys()
	time.Sleep(time.Second)
	fmt.Printf("\n")
	hashCache.Cache(1).PrintKeys()
	hashCache.Cache(0).PrintKeys()

	fmt.Printf("cache size %+v\n", hashCache.GetLen())
}

func hcache_lru_main() {
	var hashCache cache.HCacheGroup
	hashCache.Init("./food.json", ParseProto)
	hashCache.SetLruSize(10)
	for i := 0; i < 10; i++ {
		var n Node
		n.T = strconv.FormatInt(int64(i), 10)
		n.V = strconv.FormatInt(int64(i), 10)
		hashCache.HCache(uint64(i)).Add(n.T, n.V, &n)
	}

	hashCache.HCache(1).PrintKeys()
	time.Sleep(time.Second)
	hashCache.HCache(1).Get("1", "1")
	hashCache.HCache(1).PrintKeys()
	hashCache.HCache(1).DeleteMem("1")
	hashCache.HCache(1).PrintKeys()
	time.Sleep(time.Second)
	hashCache.HCache(1).PrintKeys()
}

func hcache_main() {
	var hashCache cache.HCacheGroup
	err := hashCache.Init("./food.json", ParseProto)
	hashCache.SetLruSize(10)
	//err := hashCache.InitMem(10, 1000)
	fmt.Printf("err %+v\n", err)
	var n Node
	n.T = "11"
	n.V = "12"
	err = hashCache.HCache(10).Add("10", "1", &n)
	fmt.Printf("err %+v\n", err)
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			/*go func(i int, j int) {*/
			{
				var n Node
				n.T = strconv.FormatInt(int64(i), 10)
				n.V = strconv.FormatInt(int64(j), 10)
				hashCache.HCache(uint64(i)).Set(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10), &n)
				hashCache.HCache(uint64(i)).Read(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10))
				//fmt.Printf("get cn %+v\n", cn)
				//hashCache.HCache(uint64(i)).Delete(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10))
				wg.Done()
			}
			/*}(i, j)*/
		}
	}
	for i := 1000; i > 0; i-- {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func(i int, j int) {
				var n Node
				n.T = strconv.FormatInt(int64(i), 10)
				n.V = strconv.FormatInt(int64(j), 10)
				//hashCache.HCache(uint64(i)).Add(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10), &n)
				//hashCache.HCache(uint64(i)).Get(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10))
				//fmt.Printf("v %+v\n", v)
				//hashCache.HCache(uint64(i)).Delete(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10))
				hashCache.HCache(uint64(i)).HandleEvent(strconv.FormatInt(int64(i), 10), "1", nil)
				hashCache.HCache(uint64(i)).WalkNode(strconv.FormatInt(int64(i), 10), func(hkey string, v cache.CacheNodeIf) int {
					//fmt.Printf("get node hkey %+v %+v\n", hkey, v)
					return 0
				})
				wg.Done()
			}(i, j)
		}
	}
	wg.Wait()
	fmt.Printf("handle test finished\n")
	time.Sleep(time.Second)
	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func(i int, j int) {
				var n Node
				n.T = strconv.FormatInt(int64(i), 10)
				n.V = strconv.FormatInt(int64(j), 10)
				//hashCache.HCache(uint64(i)).Add(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10), &n)
				//hashCache.HCache(uint64(i)).Get(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10))
				//fmt.Printf("v %+v\n", v)
				//hashCache.HCache(uint64(i)).Delete(strconv.FormatInt(int64(i), 10), strconv.FormatInt(int64(j), 10))
				hashCache.HCache(uint64(i)).HandleEvent(strconv.FormatInt(int64(i), 10), "1", nil)
				hashCache.HCache(uint64(i)).WalkNode(strconv.FormatInt(int64(i), 10), func(hkey string, v cache.CacheNodeIf) int {
					//fmt.Printf("get node key %+v hkey %+v %+v\n", strconv.FormatInt(int64(i), 10), hkey, v)
					return 0
				})
				wg.Done()
			}(i, j)
		}
	}
	wg.Wait()
}
