package sttparser

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/jiangdamalong/common/typeoper"
)

//var stt_str string = "a@=b/type@=msgrepeaterlist/rid@=1/list@=id@AA=7@ASnr@AA=1@ASip@AA=danmu@AS@Sid@AA=7@ASnr@AA=22@ASip@AA=danmu@AS@Sid@AA=7@ASnr@AA=33@ASip@AA=danmu@AS@S/cc@=dd/"

//var stt_str string = "a@=b/type@=msgrepeaterlist/rid@=1/list@=id@A=7@S/"
const (
	ST_SIMBOL_CHAR     = 0
	ST_SIMBOL_EQUAL    = 1
	ST_SIMBOL_SEPARATE = 2
)

//simbol queue
type StSimbolItem struct {
	level    int
	stype    int
	beginPos int
	endPos   int
	next     *StSimbolItem
	prev     *StSimbolItem
}

func (item *StSimbolItem) Init(level int, stype int, beginPos int, endPos int) {
	item.level = level
	item.stype = stype
	item.beginPos = beginPos
	item.endPos = endPos
}

//entry stack
type IndexStackSt struct {
	entry   *reflect.Value
	stTags  *typeoper.StructTag
	refType *reflect.Type
	key     string
	level   int

	beginPos int
	endPos   int

	next *IndexStackSt
	prev *IndexStackSt
}

func (s *IndexStackSt) InitStack() {
	s.next = nil
	s.prev = nil
}

func (s *IndexStackSt) Push(v *IndexStackSt) {
	v.next = s.next
	if s.next != nil {
		s.next.prev = v
	}
	s.next = v
}

func (s *IndexStackSt) PopStack() {
	if s.next != nil {
		s.next = s.next.next
	}
}

func (s *IndexStackSt) Peak() *IndexStackSt {
	return s.next
}

func (parser *SttStParser) PushStack(allocator *StackItemAllocator, entryStack *IndexStackSt, entry *reflect.Value, ktype *reflect.Type, tags *typeoper.StructTag, key string, level int, beginPos int) {
	//var ikey IndexStackSt
	ikey := allocator.Alloc()
	ikey.entry = entry
	ikey.stTags = tags
	ikey.refType = ktype
	ikey.key = key
	ikey.level = level
	ikey.beginPos = beginPos
	entryStack.Push(ikey)
	//fmt.Printf("push stack %+v %+v\n", entryStack, entry)
}

func (parser *SttStParser) PopStack(entryStack *IndexStackSt) {
	entryStack.PopStack()
	//fmt.Printf("pop stack\n")
	//	parser.poolStack.Put(v.(*IndexStackSt))
}

type SttStParser struct {
	collect typeoper.StructCollect
	//poolStack  *sync.Pool
	//poolSimbol *sync.Pool
	poolSimbol ItemManager
	poolStack  StackItemManager
	lock       sync.Mutex
	stTags     *typeoper.StructTag
	rType      reflect.Type
}

func (parser *SttStParser) Init(desc string) {
	parser.collect.Init(desc)
	parser.poolSimbol.Init(512, 10000, 20000)

	parser.poolStack.Init(64, 2000, 2000)
}

func (parser *SttStParser) GetStructTagInfo(t reflect.Type) *typeoper.StructTag {
	parser.lock.Lock()
	defer parser.lock.Unlock()
	if t == parser.rType {
	} else {
		parser.stTags = parser.collect.GetStructTagInfo(&t)
		parser.rType = t
	}
	return parser.stTags
}

func (parser *SttStParser) UnmarshalRv(entry reflect.Value, buf []byte, len int) error {
	allocator := parser.poolSimbol.GetAllocator()
	defer parser.poolSimbol.CleanAllocator(allocator)

	for {
		if entry.Kind() == reflect.Ptr {
			entry = entry.Elem()
		} else {
			break
		}
	}
	ktype := entry.Type()
	value := entry
	tags := parser.GetStructTagInfo(ktype)
	simbolList := parser.CalcuSimbolQueue(buf, len, allocator)
	return parser.unmarshal(&value, &ktype, tags, buf, simbolList)
}

func (parser *SttStParser) Unmarshal(entry interface{}, buf []byte, len int) error {
	allocator := parser.poolSimbol.GetAllocator()
	defer parser.poolSimbol.CleanAllocator(allocator)

	var ktype reflect.Type
	value := reflect.ValueOf(entry)

	for {
		if value.Type().Kind() == reflect.Ptr {
			value = value.Elem()
		} else {
			ktype = value.Type()
			break
		}
	}
	tags := parser.GetStructTagInfo(ktype)
	simbolList := parser.CalcuSimbolQueue(buf, len, allocator)
	return parser.unmarshal(&value, &ktype, tags, buf, simbolList)
}

func (parser *SttStParser) Marshal(buf *bytes.Buffer, entry interface{}) ([]byte, error) {
	//var buf bytes.Buffer
	var ktype reflect.Type
	var value reflect.Value
	value = reflect.ValueOf(entry)

	for {
		if value.Type().Kind() == reflect.Ptr {
			value = value.Elem()
		} else {
			ktype = value.Type()
			break
		}
	}
	tags := parser.GetStructTagInfo(ktype)

	parser.marshal(&value, tags, buf, 0)
	return buf.Bytes(), nil
}

func (parser *SttStParser) MarshalRv(buf *bytes.Buffer, entry reflect.Value) ([]byte, error) {
	for {
		if entry.Kind() == reflect.Ptr {
			entry = entry.Elem()
		} else {
			break
		}
	}

	if entry.Kind() != reflect.Struct {
		return nil, errors.New("Marshal Type error")
	}

	ktype := entry.Type()
	value := entry
	tags := parser.collect.GetStructTagInfo(&ktype)

	parser.marshal(&value, tags, buf, 0)
	return buf.Bytes(), nil
}

func WriteSepSimbol(buf *bytes.Buffer, level int) {
	if level == 0 {
		buf.WriteByte('/')
		return
	}
	buf.WriteByte('@')
	for i := 0; i < level-1; i++ {
		buf.WriteByte('A')
	}
	buf.WriteByte('S')
}

func WriteEqualSimbol(buf *bytes.Buffer, level int) {
	buf.WriteByte('@')
	for i := 0; i < level; i++ {
		buf.WriteByte('A')
	}
	buf.WriteByte('=')
}

func TransStringValueWithMetacharacter(buf string, level int) string {

	result := make([]byte, 0)
	for _, v := range []byte(buf) {
		if v == '/' {
			result = append(result, '@')
			for i := 0; i <= level; i++ {
				result = append(result, 'A')
			}
			result = append(result, 'S')
		} else if v == '@' {
			result = append(result, '@')
			for i := 0; i <= level; i++ {
				result = append(result, 'A')
			}
			result = append(result, 'A')
		} else {
			result = append(result, v)
		}
	}
	return string(result)
}

func TransStringValueBackToMetacharacter(buf []byte) string {
	// 將转义后的元字符还原
	ret := make([]byte, 0, 1024)
	for i := 0; i < len(buf); i++ {
		v := buf[i]
		if v == '@' {
			i++
			flag := false
			for ; i < len(buf); i++ {
				tmp := buf[i]
				if tmp == 'S' {
					ret = append(ret, '/')
					break
				} else if tmp == 'A' {
					flag = true
					continue
				}
				if flag {
					ret = append(ret, '@')
				}
				i--
				break
			}
			continue
		}
		ret = append(ret, v)
	}

	return string(ret)
}

func (parser *SttStParser) marshal(entry *reflect.Value, entryTags *typeoper.StructTag, buf *bytes.Buffer, level int) error {
	/*entryKind := (*entry).Kind()
	for entryKind == reflect.Ptr {
		if entryKind == reflect.Ptr {
			tentry := (*entry).Elem()
			entry = &tentry
			entryKind = tentry.Kind()
		} else if entryKind == reflect.Struct {
			entryType := (*entry).Type()
			entryTags = parser.collect.GetStructTagInfo(&entryType)
			break
		} else {
			//entryTags = nil
			break
		}
	}*/

	for entry.Kind() == reflect.Ptr {
		*entry = (*entry).Elem()
	}

	var ret_err error
	ret_err = nil
	//entryKind := (*entry).Type().Kind()

	var rtype reflect.Type
	var rtags *typeoper.StructTag
	for i := 0; i < (*entry).NumField(); i++ {
		if entryTags.IndexTags[i] == nil {
			continue
		}
		var itag *typeoper.TagInfo
		var ivalue reflect.Value
		var ikind reflect.Kind
		var ielemkind reflect.Kind

		itag = entryTags.IndexTags[i]
		//ikind = itag.field.Type.Kind()
		ivalue = entry.Field(i)
		ielemkind = itag.ElemKind

		if ivalue.Kind() == reflect.Ptr {
		}

		for {
			if ivalue.Kind() == reflect.Ptr {
				ivalue = ivalue.Elem()
			} else {
				break
			}
		}
		ikind = ivalue.Kind()
		if ikind == reflect.Invalid {
			continue
		}

		itype := ivalue.Type()
		if ikind == reflect.Struct {
			if itype != rtype {
				rtags = parser.collect.GetStructTagInfo(&itype)
				rtype = itype
			}
			itagsst := rtags
			buf.WriteString(itag.TagName)
			WriteEqualSimbol(buf, level)
			parser.marshal(&ivalue, itagsst, buf, level+1)
			WriteSepSimbol(buf, level)
		} else if ikind == reflect.Slice && ielemkind != reflect.Uint8 {
			if ivalue.Len() == 0 {
				continue
			}
			buf.WriteString(itag.TagName)
			WriteEqualSimbol(buf, level)

			valtmp := ivalue.Index(0)
			itypetmp := valtmp.Type()
			ikeytype := itypetmp.Kind()

			for j := 0; j < ivalue.Len(); j++ {
				iv := ivalue.Index(j)
				//fmt.Printf("%+v %+v %+v\n", iv, ikeytype, itypetmp.Elem().Kind())
				if ikeytype == reflect.Struct || ikeytype == reflect.Ptr ||
					(ikeytype == reflect.Slice && itypetmp.Elem().Kind() != reflect.Uint8) {
					//itags := parser.collect.GetStructTagInfo(&itypetmp)

					if j == 0 {
						rtags = parser.collect.GetStructTagInfo(&itypetmp)
						rtype = itypetmp
					}
					itags := rtags

					parser.marshal(&iv, itags, buf, level+2)
					WriteSepSimbol(buf, level+1)
				} else {
					fixkeytype := ikeytype
					if ikeytype == reflect.Slice && itypetmp.Elem().Kind() == reflect.Uint8 {
						fixkeytype = reflect.String
					}
					vstr, err := entryTags.GetBasicValue(&iv, fixkeytype)
					if err == nil {
						buf.WriteString(TransStringValueWithMetacharacter(vstr, level))
						WriteSepSimbol(buf, level+1)
					} else {
						ret_err = err
					}
				}
			}
			WriteSepSimbol(buf, level)
		} else {
			if ikind == reflect.Slice && ielemkind == reflect.Uint8 {
				ikind = reflect.String
			}
			vstr, err := entryTags.GetBasicValue(&ivalue, ikind)
			if err == nil {
				buf.WriteString(itag.TagName)
				WriteEqualSimbol(buf, level)
				buf.WriteString(TransStringValueWithMetacharacter(vstr, level))
				WriteSepSimbol(buf, level)
			}
		}
	}
	return ret_err
}

func (parser *SttStParser) unmarshal(rootEntry *reflect.Value, rootType *reflect.Type, rootTags *typeoper.StructTag, buf []byte, simbolList *StSimbolItem) error {
	//	rootEntry := make(map[string]interface{})
	allocator := parser.poolStack.GetAllocator()
	defer parser.poolStack.CleanAllocator(allocator)

	nowLevel := 0
	preBufpos := 0

	var nowEntry *IndexStackSt

	entryStack := allocator.Alloc()
	entryStack.InitStack()

	parser.PushStack(allocator, entryStack, rootEntry, rootType, rootTags, "", 0, 0)
	nowEntry = entryStack.Peak()

	var nowkey string
	var prekey string
	var value string

	var rtype reflect.Type
	var rtags *typeoper.StructTag
	var nowKeyKind reflect.Kind
	var strLevel int
	for e := simbolList.next; e != nil; e = e.next {
		//item := e.Value.(*StSimbolItem)
		e.prev.next = nil
		e.prev = nil
		item := e
		if strLevel < item.level && nowKeyKind == reflect.String {
			continue
		}

		if item.stype == ST_SIMBOL_EQUAL && preBufpos < item.beginPos {
			tentry := entryStack.Peak()
			nowkey = string(buf[preBufpos:item.beginPos])
			if tentry.stTags != nil {
				nowKeyKind = tentry.stTags.GetStructElemKind(tentry.entry, nowkey)
			} else {
				nowKeyKind = reflect.Invalid
			}

			if nowKeyKind == reflect.String {
				strLevel = item.level
				preBufpos = item.endPos + 1
			}
		}

		if item.level > nowLevel {
			for i := nowLevel; i < item.level; i++ {
				tentry := entryStack.Peak()
				if tentry.entry == nil {
					parser.PushStack(allocator, entryStack, nil, nil, nil, "", item.level, preBufpos)
				} else {
					tkind := (*tentry.refType).Kind()
					if tkind == reflect.Struct {
						v, t := tentry.stTags.GetStructElem(tentry.entry, prekey)
						if t != nil && *t != rtype {
							rtags = parser.collect.GetStructTagInfo(t)
							rtype = *t
						}
						tags := rtags
						parser.PushStack(allocator, entryStack, v, t, tags, prekey, i, preBufpos)
					} else if tkind == reflect.Slice || tkind == reflect.Array {
						v, t := tentry.stTags.AllocSliceElem(tentry.entry)
						if t != nil && *t != rtype {
							rtags = parser.collect.GetStructTagInfo(t)
							rtype = *t
						}
						tags := rtags
						//tags := parser.collect.GetStructTagInfo(t)
						parser.PushStack(allocator, entryStack, v, t, tags, prekey, i, preBufpos)
					} else {
						parser.PushStack(allocator, entryStack, nil, nil, nil, "", i, preBufpos)
					}
				}
				nowEntry = entryStack.Peak()
			}
		}

		prekey = nowkey
		if item.stype == ST_SIMBOL_SEPARATE {
			if preBufpos < item.beginPos {
				if nowEntry.entry != nil {
					value = TransStringValueBackToMetacharacter(buf[preBufpos:item.beginPos])
					nowKind := (*nowEntry.refType).Kind()
					if nowEntry.entry != nil && nowKind == reflect.Struct {
						nowEntry.stTags.SetElemValue(nowEntry.entry, nowkey, value)
					}
					if nowEntry.entry != nil && (nowKind == reflect.Array || nowKind == reflect.Slice) {
						nowEntry.stTags.AppendBasicArrayElem(nowEntry.entry, value)
					}
				}
				prekey = ""
				nowkey = ""
			}
			if nowLevel > item.level {
				for i := nowLevel; i > item.level; i-- {
					if entryStack.Peak() == nil {
						fmt.Printf("get a nil %v %v\n", nowLevel, item.level)
					}
					parser.PopStack(entryStack)
					nowEntry = entryStack.Peak()
				}
			}
		}

		nowLevel = item.level
		preBufpos = item.endPos + 1
	}
	return nil
}

//建立符号队列
func (parser *SttStParser) CalcuSimbolQueue(buf []byte, blen int, allocator *ItemAllocator) *StSimbolItem {
	i := 0
	preLevel := 0
	//lsimbol := list.New()
	//var lsimbol *StSimbolItem
	simbolHead := allocator.Alloc()
	simbolHead.prev = simbolHead
	simbolHead.next = simbolHead
	simbolNow := simbolHead

	for i < blen {
		c := buf[i]
		//sitem := parser.poolSimbol.Get().(*StSimbolItem)
		//	var sitem StSimbolItem
		sitem := allocator.Alloc()
		switch c {
		case '/':
			sitem.level = 0
			sitem.beginPos = i
			sitem.endPos = i
			sitem.stype = ST_SIMBOL_SEPARATE
			//lsimbol.PushBack(sitem)
			simbolNow.next = sitem
			sitem.prev = simbolNow
			sitem.next = nil
			simbolNow = sitem
			i++
		case '@':
			sitem.beginPos = i
			sitem.stype = ST_SIMBOL_CHAR
			level_t := 0
			i++
		for_s_loop:
			for i < blen {
				cs := buf[i]
				switch cs {
				case 'A':
					level_t++
					//if level_t <= preLevel {
					//	sitem.stype = ST_SIMBOL_SEPARATE
					//}
				case '=':
					sitem.stype = ST_SIMBOL_EQUAL
					preLevel = level_t
					break for_s_loop
				case 'S':
					level_t++
					if level_t <= (preLevel + 1) {
						sitem.stype = ST_SIMBOL_SEPARATE
					}
					break for_s_loop
				default:
					break for_s_loop
				}
				i++
			}
			if sitem.stype != ST_SIMBOL_CHAR {
				sitem.endPos = i
				sitem.level = level_t
				//lsimbol.PushBack(sitem)
				simbolNow.next = sitem
				sitem.prev = simbolNow
				sitem.next = nil
				simbolNow = sitem
			}
			break
		default:
			i++
			continue
		}
	}
	//add a end elem
	sitem := allocator.Alloc()
	sitem.level = 0
	sitem.beginPos = blen
	sitem.endPos = blen
	sitem.stype = ST_SIMBOL_SEPARATE
	simbolNow.next = sitem
	sitem.prev = simbolNow
	sitem.next = nil
	simbolNow = sitem

	return simbolHead
}
