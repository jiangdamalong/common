package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"rpcframe/server/sttparser"
	"strings"
)

type NlElem struct {
	Icon []string `protobuf:"bytes,1,rep,name=icon" json:"icon,omitempty" stt:"icon"`
	Lv   int32    `protobuf:"varint,2,opt,name=lv" json:"lv,omitempty" stt:"lv"`
	Ne   int32    `protobuf:"varint,3,opt,name=ne" json:"ne,omitempty" stt:"ne"`
	Nn   string   `protobuf:"bytes,4,opt,name=nn" json:"nn,omitempty" stt:"nn"`
	Pg   int32    `protobuf:"varint,5,opt,name=pg" json:"pg,omitempty" stt:"pg"`
	Rg   int32    `protobuf:"varint,6,opt,name=rg" json:"rg,omitempty" stt:"rg"`
	Rk   int32    `protobuf:"varint,7,opt,name=rk" json:"rk,omitempty" stt:"rk"`
	Sahf int32    `protobuf:"varint,8,opt,name=sahf" json:"sahf,omitempty" stt:"sahf"`
	Uid  int32    `protobuf:"varint,9,opt,name=uid" json:"uid,omitempty" stt:"uid"`
}

type NlList struct {
	Nl   []*NlElem `protobuf:"bytes,1,rep,name=nl" json:"nl,omitempty" stt:"nl"`
	Num  int32     `protobuf:"varint,2,opt,name=num" json:"num,omitempty" stt:"num"`
	Rid  int32     `protobuf:"varint,3,opt,name=rid" json:"rid,omitempty" stt:"rid"`
	Type string    `protobuf:"bytes,4,opt,name=type" json:"type,omitempty" stt:"type"`
}

func jsonPrettyPrint(in string) string {
	var out bytes.Buffer
	err := json.Indent(&out, []byte(in), "", "  ")
	if err != nil {
		return in
	}
	return out.String()
}

func main() {
	//var buf = []byte("type@=frank/fc@=225/bnn@=瓜咕咕/list@=uid@AA=1277177@ASnn@AA=污瓜皮皮@ASic@AA=avanew@AASface@AAS201804@AAS4fb3a9f58a11fce96bd210d66773154e@ASfim@AA=53513900@ASbl@AA=27@ASlev@AA=51@ASpg@AA=1@ASrg@AA=4@ASnl@AA=1@ASsahf@AA=0@AS@Suid@AA=109268988@ASnn@AA=静安赵tie柱@ASic@AA=avanew@AASface@AAS201706@AAS24@AAS00@AAS15e855b7c1085486a48ca72fcc25b389@ASfim@AA=15782800@ASbl@AA=22@ASlev@AA=49@ASpg@AA=1@ASrg@AA=4@ASsahf@AA=0@AS@Suid@AA=12128449@ASnn@AA=Nttyloo@ASic@AA=avatar@AASdefault@AAS09@ASfim@AA=10053700@ASbl@AA=20@ASlev@AA=40@ASpg@AA=1@ASrg@AA=4@ASsahf@AA=0@AS@Suid@AA=79832046@ASnn@AA=亦无易@ASic@AA=avanew@AASface@AAS201801@AAS27@AAS15@AAS33c15c641f7de3d12d35a13587719cf0@ASfim@AA=9859600@ASbl@AA=20@ASlev@AA=41@ASpg@AA=1@ASrg@AA=4@ASnl@AA=3@ASsahf@AA=0@AS@Suid@AA=56428632@ASnn@AA=Ghosttttt1@ASic@AA=avatar@AASface@AAS201607@AAS04@AASa3feabd0a498b543b625156a2c22c43e@ASfim@AA=7128700@ASbl@AA=19@ASlev@AA=46@ASpg@AA=1@ASrg@AA=4@ASsahf@AA=0@AS@Suid@AA=145060585@ASnn@AA=我不是蛋蛋凉了/")
	//var buf = []byte("uid@=198370751/rid@=591284/ec@=0/surl@=https:@S@Shdlsa.douyucdn.cn@Slive@S591284rrQ3Fu0ewr.flv?wsAuth=4c0679452a9fb727bb3bc01523dfb593&token=web-douyu-198370751-591284-a39a46264406ec251342fb328eeb29ff&logo=0&expire=0&did=a906219d53da4130c574601500001501&ver=Douyu_H5_2018070701beta&pt=2&st=0/cdn@=ws-h5/isp2p@=1/did@=a906219d53da4130c574601500001501/ps@=2/ct@=0/pt@=2/ext@={\"bt\":0,\"ex\":\"\",\"pn\":\"tct-h5\",\"dl\":31165610,\"tdl\":841189575,\"pdl\":0,\"pcdl\":32718048}/pid@=27111531121934633/uip@=58.49.117.118/aver@=Douyu_H5_2018070701beta/type@=ssr/")
	f := "stt.txt"
	for i, arg := range os.Args {
		if arg == "-f" {
			f = os.Args[i+1]
		}
	}

	buf, _ := ioutil.ReadFile(f)

	arrstt := strings.Split(string(buf), "\n")
	var parse sttparser.SttStParser
	parse.Init("stt")
	for _, buf := range arrstt {
		if buf == "" {
			continue
		}
		m, _ := sttparser.ParseToMap([]byte(buf))
		js, _ := json.Marshal(m)

		/*var list NlList
		parse.Unmarshal(&list, []byte(buf), len(buf))

		var b bytes.Buffer
		by, _ := parse.Marshal(&b, &list)
		fmt.Printf("%+v\n", string(by))*/
	}
}
