package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jiangdamalong/common/log"
	"github.com/jiangdamalong/common/taskmgr"
)

func test() {
	defer func() {
		fmt.Printf("catch here___\n")
		recover()
		fmt.Printf("catch here\n")
		return
	}()
	for {
		fmt.Printf("fuck here\n")
		panic("ttttt")
		fmt.Printf("fuck here1\n")
	}
}

func RunCall(req int, resp *int) int {
	fmt.Printf("hello %+v req %+v resp\n", req, *resp)
	return req + *resp
}

type A struct {
	A int
}

func (a *A) Test(ar int) int {
	time.Sleep(time.Second * 3)
	return a.A
}

/*
func (a A) String() string {
	log.Debugf("11111222222222222222")
	return "AAAAAAAAAAAAAAA"
}*/

func LL2(v int) string {
	log.Debugf("ttttttt3 %+v", v)
	return "3"
}

func LL(v int) string {
	log.Debugf("ttttt1 %+v %+v", v, LL2(v))
	return "2"
}

func GTest(t *taskmgr.HTaskManager) {
	fmt.Printf("test stuck\n")
	t.Call(11, func() {
		fmt.Printf("test stuck2\n")
	})
}

func main() {
	var htask taskmgr.HTaskManager
	log.SetLevelString("debug")
	var wg sync.WaitGroup
	htask.Start(300000, 100)
	hg := htask.FetchTaskGroup()
	//tb := time.Now()
	n := 100
	m := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < m; j++ {
				wg.Add(1)
				htask.CallCtx(nil, uint64(i), func(ctx *taskmgr.TaskCtx, i int, j int) {
					fmt.Printf("ffff1\n")
					hg.CallCtx(ctx, uint64(j), func(ctx *taskmgr.TaskCtx, i int, j int) {
						//go func(j int) (A, int) {
						//test()
						fmt.Printf("ffff2\n")
						if j == 1 {
							//panic("ffff")
						}
						//htask.CallR(1)
						/*htask.CallR(func(i int, j int) {
							htask.CallCtx(ctx, uint64(i), func(ctx *taskmgr.TaskCtx, i int, j int) {
								htask.CallCtx(ctx, uint64(i), func(ctx *taskmgr.TaskCtx, i int, j int) {
									//fmt.Printf("2handle call go %+v %+v\n", i*10000000+j, ctx)
									//ctx.Print()
								}, i, j)
								//fmt.Printf("handle call go %+v %+v\n", i*10000000+j, ctx)
							}, i, j)
							//ctx.Print()
							wg.Done()
							//return nil, 0
						}, i, j)*/
						wg.Done()
					}, i, j)
					//wg.Done()
					fmt.Printf("handle i %+v j %+v\n", i, j)
				}, i, j)
			}
			wg.Done()
		}(i)
	}

	/*htask.CallCtx(nil, 1, func(ctx *taskmgr.TaskCtx) {
		fmt.Printf("call 0 %+v\n", ctx)
		htask.CallCtx(ctx, 1, func(ctx *taskmgr.TaskCtx) {
			fmt.Printf("call 1\n")
			htask.CallCtx(ctx, 1, func(ctx *taskmgr.TaskCtx) {
				fmt.Printf("call 2\n")
			})
		})
		htask.CallCtx(ctx, 1, func(ctx *taskmgr.TaskCtx) {
			fmt.Printf("call 11\n")
			htask.CallCtx(ctx, 2, func(ctx *taskmgr.TaskCtx) {
				fmt.Printf("call 12\n")
				htask.CallCtx(ctx, 2, func(ctx *taskmgr.TaskCtx) {
					fmt.Printf("call 112\n")
				})
			})
		})
		fmt.Printf("call 3\n")
	})*/
	/*htask.Call(o.Copy(1), GTest, &htask)
	GTest(&htask)*/
	hg.Wait()
	wg.Wait()
	//fmt.Printf("cost time %+v\n", int64(n*m)/(time.Now().Unix()-tb.Unix()))
	runtime.GC()
	debug.FreeOSMemory()
	time.Sleep(time.Second * 5)
	//fmt.Printf("%+v\n", htask)
	//fmt.Printf("%+v\n", htask)
	//time.Sleep(20 * time.Second)
	//var a A

	/*var taskm taskmgr.TaskManager
	taskm.Start(3, 10, false)
	var tg taskmgr.TaskGroup
	tg.Init(&taskm)

	var a A
	for i := 0; i < 100; i++ {
		tg.AddFuncS(func(i int, a *A, b A) {
			fmt.Printf("%+v %+v %+v\n", i, a, b)
		}, i, &a, a)
	}
	tg.CallFuncsS()
	fmt.Printf("call finished\n")
	//for i := 0; i < 10; i++ {
	/*log.Debugf("%+v %+v", a, 1)
	log.Debugf("%+v %+v", a, 1)
	log.Debugf("%+v %+v", a, 1)
	log.Debugf("%+v %+v", a, 1)
	//}
	time.Sleep(2 * time.Second)*/
}
