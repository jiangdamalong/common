package main

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"

	"github.com/jiangdamalong/common/sttparser"
	"github.com/jiangdamalong/common/typeoper"
	//"os"
	//"runtime/pprof"
	"time"
)

//var stt_str string = "a@=b/type@=msgrepeaterlist/rid@=1/list@=id@AA=7@ASnr@AA=1@ASip@AA=danmu@ASaa@ASbb@AS@Sid@AA=7@ASnr@AA=22@ASip@AA=danmu@AS@Sid@AA=7@ASnr@AA=33@ASip@AA=danmu@AS@S/cc@=dd/ee/ff/"

//var stt_str string = "a@=b/type@=msgrepeaterlist/rid@=1/list@=@AAA=7@AAS8@AAS9@AAS@AS@Snr@AA=1@ASip@AA=danmu@ASaa@ASbb@AS@Sid@AA=7@ASnr@AA=22@ASip@AA=danmu@AS@Sid@AA=7@ASnr@AA=33@ASip@AA=danmu/cc@=s///////"

var stt_str string = "a@=bbb/c@=1/type@=msgrepeaterlist/G@=D@A=10@SFF@A=1@AS@SAd@A=1.2@AS1.3@AS@S/F@=D@AA=15@ASFF@AA=122@AAS@ASAd@AA=1.223@AAS@AS@SD@AA=14@ASFF@AA=122@AAS@ASAd@AA=1.224@AAS@AS@SD@AA=13@ASFF@AA=122@AAS@ASAd@AA=1.225@AAS1.226@AAS@AS@S/Kk@=1.2/"

//var stt_str string = "type@=chatmsg/db@=chat_msg_2018_9_28/coll@=77592072/obj@={\"_id\":\"f24ecab57ee944c34c57000000000000\",\"aid\":0,\"aver\":2016101703,\"c_ts\":0,\"col\":0,\"content\":\"1538113799\",\"ct\":1,\"did\":\"44FAE0E784F5865E1F2EB6A6A7C3C683\",\"dmva\":\"\",\"dmvr\":1,\"fans\":0,\"hlink\":\"\",\"icon\":\"avatar@Sdefault@S11\",\"ip\":\"10.117.24.140\",\"legal\":1,\"nick\":\"test201660000124\",\"noble\":0,\"p2p\":0,\"pg\":1,\"rd\":0,\"rg\":1,\"rid\":77592072,\"sm_did\":\"44FAE0E784F5865E1F2EB6A6A7C3C683\",\"ts\":1538113790,\"u_level\":4,\"uid\":60000124,\"ver\":20150929}"

type SttStruct struct {
	A [][]byte `protobuf:"bytes,1,req,name=message" json:"message,omitempty" stt:"test_uid"`
	//A    [][]byte `protobuf:"bytes,1,req,name=message" json:"message,omitempty"`
	C    int32      `stt:"c"`
	TYPE string     `stt:"type"`
	G    SttChild   `stt:"fuck"`
	F    []SttChild `stt:"F"`
	Kk   float32    `stt:"Kk"`
	V    string     `stt:"value"`
}

type SttChild struct {
	D        *int         `stt:"D"`
	FF       []int        `stt:"FF"`
	Ad       []float32    `stt:"Ad"`
	KK       *int         `stt:"Kk"`
	GrandSon []SttGranSon `stt:"GrandSon"`
}

type SttGranSon struct {
	A  string   `stt:"A"`
	B  string   `stt:"B"`
	CC int      `stt:"CC"`
	DD [][]byte `stt:"DD"`
}

//var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
//var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

type MqDanmu struct {
	Obj string `stt:"obj"`
}

type MqDanmuObj struct {
	UserId uint64 `json:"uid"`
	RoomId uint64 `json:"rid"`
}

data := "type@=chatmsg/db@=chat_msg_2018_9_2/coll@=309312/obj@={"uid": 1, "ip": "10.118.26.118", "rid": 306384, "hlink": "", "ct": 28, "rd": 0, "ver": 20180528, "u_level": 38, "ts": 1540713303, "legal": 1, "content": "13063842018-10-28 15:55:03.775000", "nick": "yyf40", "fans": 0, "rg": 5, "pg": 1, "aver": 3, "noble": 0, "p2p": 0, "icon": "avatar@Sdefault@S11", "sm_did": "1C:1B:0D:E5:3F:74", "dmvr": 1, "did": "1C:1B:0D:E5:3F:74", "c_ts": 0, "aid": 0, "dmva": "", "_id": "bf25513220f94b3d7f00000000000000", "col": 0}"

func main() {
	/*	flag.Parse()
		if *cpuprofile != "" {
			f, err := os.Create(*cpuprofile)
			if err != nil {
				log.Fatal("could not create CPU profile: ", err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				log.Fatal("could not start CPU profile: ", err)
			}
			defer pprof.StopCPUProfile()
		}*/

	var t SttStruct
	for i := 0; i < 20; i++ {
		t.A = append(t.A, []byte("abc"))
	}
	var iv = 11
	t.C = 12
	t.TYPE = "msgrepeaterlist"
	t.G.D = &iv
	t.G.FF = []int{1, 2, 3, 4, 1, 2, 3, 6}
	t.G.Ad = []float32{1.3, 1.5, 1.444, 1.555}
	t.G.KK = &iv
	t.V = "mary had a little lamb,little lamb,mary had a little lamb its"

	var grandson []SttGranSon
	for i := 0; i < 10; i++ {
		var grand SttGranSon
		grand.A = "test1"
		grand.B = "test2"
		grand.CC = i
		for j := 0; j < 3; j++ {
			grand.DD = append(grand.DD, []byte("cccddd"))
			//grand.DD = append(grand.DD, "cccddd")
		}
		grand.DD = append(grand.DD, []byte("eeeee"))
		grandson = append(grandson, grand)
	}
	t.G.GrandSon = grandson

	var child []SttChild
	for i := 0; i < 10; i++ {
		var ichild SttChild
		ichild.D = &iv
		ichild.FF = []int{i, 3, 4, 5, i}
		ichild.Ad = []float32{1.1, 1.2, 3.3, 4.4}
		ichild.KK = &iv
		ichild.GrandSon = grandson
		child = append(child, ichild)
	}
	t.F = child
	t.Kk = 1.333

	var parser sttparser.SttStParser
	parser.Init("stt")

	var iobuf bytes.Buffer
	buf1, err := parser.Marshal(&iobuf, &t)
	fmt.Printf("%+v\n", t)
	fmt.Printf("%+v %+v\n", string(buf1), err)

	t_now := time.Now()
	parser.Init("stt")
	timebegin := t_now.UTC().Unix()
	buf_tmp := []byte(buf1[0:len(buf1)])
	var wg sync.WaitGroup
	for ir := 0; ir < 1; ir++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t1 := new(SttStruct)
			for i := 0; i < 10000; i++ {
				//t1 = t_empty
				//var t1 SttStruct
				//fmt.Printf("%+v\n", t1)
				parser.Unmarshal(t1, buf_tmp, len(buf_tmp))
				typeoper.ClearStruct(reflect.ValueOf(t1), reflect.TypeOf(t1))
				//fmt.Printf("%+v\n", t1)
			}
		}()
	}
	wg.Wait()
	t_now = time.Now()
	timesafter := t_now.UTC().Unix()
	fmt.Printf("spend time %+v\n", (timesafter - timebegin))

	/*	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}*/
}
