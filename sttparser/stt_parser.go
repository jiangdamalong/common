package sttparser

import (
	//stten "dyrpc/json"
	"container/list"
	"fmt"
	"reflect"
)

//var stt_str string = "a@=b/type@=msgrepeaterlist/rid@=1/list@=id@AA=7@ASnr@AA=1@ASip@AA=danmu@AS@Sid@AA=7@ASnr@AA=22@ASip@AA=danmu@AS@Sid@AA=7@ASnr@AA=33@ASip@AA=danmu@AS@S/cc@=dd/"

//var stt_str string = "a@=b/type@=msgrepeaterlist/rid@=1/list@=id@A=7@S/"
const (
	SIMBOL_CHAR     = 0
	SIMBOL_EQUAL    = 1
	SIMBOL_SEPARATE = 2

	KEY_STRING = 3
	KEY_INDEX  = 4
	KEY_ROOT   = 5
	KEY_EMPTY  = 6
)

type SimbolItem struct {
	level     int
	stype     int
	begin_pos int
	end_pos   int
}

//索引队列
type IndexStack struct {
	entry        interface{}
	key          string
	index        int
	ktype        int
	level        int
	is_arr_enter int
}

func AddKeyItem(entry_stack *Stack, entry interface{}, key string, index int, ktype int, level int, is_arr_enter int) {
	var ikey IndexStack
	ikey.entry = entry
	ikey.key = key
	ikey.index = index
	ikey.ktype = ktype
	ikey.level = level
	ikey.is_arr_enter = is_arr_enter
	entry_stack.Push(ikey)
}

func PeakEntry(entry_stack *Stack) interface{} {
	e := entry_stack.Peak()
	if e == nil {
		return nil
	}
	return e.(IndexStack).entry
}

func GetItemMap(buf []byte, slist *list.List) (map[string]interface{}, error) {
	root_entry := make(map[string]interface{})

	now_level := 0

	pre_buf_pos := 0

	var now_entry interface{}
	now_entry = root_entry
	entry_stack := NewStack()
	AddKeyItem(entry_stack, now_entry, "", 0, KEY_ROOT, 0, 0)

	var nowkey string
	var prekey string
	var value string

	for e := slist.Front(); e != nil; e = e.Next() {
		item := e.Value.(SimbolItem)
		if item.stype == SIMBOL_EQUAL && pre_buf_pos < item.begin_pos {
			nowkey = string(buf[pre_buf_pos:item.begin_pos])
		}
		//level increase, create a map or array
		if item.level == now_level+1 {
			//a level down, check now node type
			//for map , assign value into map
			//for array, make a empty slice to accept coming values,maybe empty
			tentry := PeakEntry(entry_stack)
			if reflect.ValueOf(tentry).Kind() == reflect.Map {
				now_entry.(map[string]interface{})[prekey] = make(map[string]interface{})
				now_entry = now_entry.(map[string]interface{})[prekey]
				AddKeyItem(entry_stack, now_entry, prekey, 0, KEY_STRING, item.level, 0)
				prekey = ""
			} else {
				//array entry in the entry stack,is the ptr to ptr
				entry_now := make(map[string]interface{})
				t_array := *tentry.(*[]interface{})
				t_array = append(t_array, entry_now)
				*(tentry.(*[]interface{})) = t_array

				now_entry = t_array[len(t_array)-1]
				//entry_stack.Push(now_entry)
				AddKeyItem(entry_stack, now_entry, "", len(t_array)-1, KEY_INDEX, item.level, 0)
			}
		}

		//@= to @AA=, no middle level,treat as array
		if item.level > now_level+1 {
			tentry := PeakEntry(entry_stack)
			for i := now_level; i < item.level-1; i++ {
				t_array := make([]interface{}, 1)
				//first, check the begin level type, different handle for array or map
				if i == now_level && reflect.ValueOf(tentry).Kind() == reflect.Map {
					now_entry.(map[string]interface{})[prekey] = &t_array
					AddKeyItem(entry_stack, now_entry.(map[string]interface{})[prekey], prekey, 0, KEY_STRING, i, 1)
					prekey = ""
				} else {
					(*now_entry.(*[]interface{}))[0] = &t_array
					AddKeyItem(entry_stack, (*now_entry.(*[]interface{}))[0], "", 0, KEY_INDEX, i, 1)
				}
				now_entry = PeakEntry(entry_stack)
			}

			//the last level, array or map? check the next simbol
			//equal means map , sep means array
			if e.Next() != nil && e.Next().Next() != nil {
				item_next := e.Next().Next().Value.(SimbolItem)
				var t_node interface{}
				var isArr = 0
				var keyType = 0
				if item_next.stype == SIMBOL_EQUAL {
					t_node = make(map[string]interface{})
					isArr = 0
					keyType = KEY_STRING
				} else {
					t_node_arr := make([]interface{}, 0)
					t_node = &t_node_arr
					isArr = 1
					keyType = KEY_INDEX
				}
				(*now_entry.(*[]interface{}))[0] = t_node
				AddKeyItem(entry_stack, (*now_entry.(*[]interface{}))[0], "", 0, keyType, item.level, isArr)
				now_entry = t_node
			}
		}
		prekey = nowkey

		if item.stype == SIMBOL_SEPARATE {
			if e.Next() != nil {
				next_item := e.Next().Value.(SimbolItem)
				//here we get a string array,fetch it
				if next_item.stype == SIMBOL_SEPARATE && e.Prev() != nil && e.Prev().Value.(SimbolItem).level == item.level-1 {
					entry_stack.Pop()
					now_entry = PeakEntry(entry_stack)
				}
				if next_item.stype == SIMBOL_SEPARATE && next_item.level >= item.level {
					var arr []string
					var val_last interface{}
					e1 := e
				for_sep_run:
					for ; e1 != nil; e1 = e1.Next() {
						t_item := e1.Value.(SimbolItem)
						if t_item.stype == SIMBOL_SEPARATE && t_item.level >= item.level {
							if pre_buf_pos < t_item.begin_pos {
								arr = append(arr, string(buf[pre_buf_pos:t_item.begin_pos]))
							}
							pre_buf_pos = t_item.end_pos + 1
						} else {
							break for_sep_run
						}
					}

					if e1 != nil {
						t_item := e1.Value.(SimbolItem)
						if t_item.stype == SIMBOL_SEPARATE {
							if pre_buf_pos < t_item.begin_pos {
								arr = append(arr, string(buf[pre_buf_pos:t_item.begin_pos]))
							}
							pre_buf_pos = t_item.end_pos + 1
						}
						e1 = e1.Next()
					}

					if len(arr) == 1 {
						val_last = arr[0]
					} else {
						val_last = arr
					}
					if reflect.ValueOf(now_entry).Kind() == reflect.Map {
						now_entry.(map[string]interface{})[nowkey] = val_last
					} else {
						t_array := *now_entry.(*[]interface{})
						t_array = append(t_array, val_last)
						*(now_entry.(*[]interface{})) = t_array
					}
					if e1 == nil {
						return root_entry, nil
					}
					e = e1.Prev()
					item = e.Value.(SimbolItem)
					prekey = ""
					nowkey = ""
				}
			}
			if pre_buf_pos < item.begin_pos {
				value = string(buf[pre_buf_pos:item.begin_pos])
				if reflect.ValueOf(now_entry).Kind() == reflect.Map {
					now_entry.(map[string]interface{})[nowkey] = value
				} else {
					t_array := *now_entry.(*[]interface{})
					t_array = append(t_array, value)
					*(now_entry.(*[]interface{})) = t_array
				}
				prekey = ""
				nowkey = ""
			}
			if now_level > item.level {
				for i := now_level; i > item.level; i-- {
					if entry_stack.Peak() == nil {
						fmt.Printf("get a nil %v %v\n", now_level, item.level)
					}
					t_index := entry_stack.Peak().(IndexStack)
					entry_stack.Pop()
					now_entry = PeakEntry(entry_stack)
					//a sad story, i don't know when zhe array finish, so, zhe array entry is a ptr for ptr. when pop happy, check and fix it
					if t_index.is_arr_enter == 1 {
						if t_index.ktype == KEY_INDEX {
							(*now_entry.(*[]interface{}))[t_index.index] = *(t_index.entry.(*[]interface{}))
						} else if t_index.ktype == KEY_STRING {
							now_entry.(map[string]interface{})[t_index.key] = *(t_index.entry.(*[]interface{}))
						}
					}
				}
			}
		}

		now_level = item.level
		pre_buf_pos = item.end_pos + 1
	}
	return root_entry, nil
}

//建立符号队列
func CalcuSimbolStack(buf []byte, len int) *list.List {
	i := 0
	lsimbol := list.New()
	for i < len {
		c := buf[i]
		switch c {
		case '/':
			var sitem SimbolItem
			sitem.level = 0
			sitem.begin_pos = i
			sitem.end_pos = i
			sitem.stype = SIMBOL_SEPARATE
			lsimbol.PushBack(sitem)
			i++
		case '@':
			var sitem SimbolItem
			sitem.begin_pos = i
			sitem.stype = SIMBOL_CHAR
			level_t := 0
		for_s_loop:
			for i < len {
				i++
				cs := buf[i]
				switch cs {
				case 'A':
					level_t++
				case '=':
					sitem.stype = SIMBOL_EQUAL
					break for_s_loop
				case 'S':
					level_t++
					sitem.stype = SIMBOL_SEPARATE
					break for_s_loop
				default:
					break for_s_loop
				}
			}
			if sitem.stype != SIMBOL_CHAR {
				sitem.end_pos = i
				sitem.level = level_t
				lsimbol.PushBack(sitem)
			}
			break
		default:
			i++
			continue
		}
	}

	return lsimbol
}

func ParseToMap(buf []byte) (map[string]interface{}, error) {
	lis := CalcuSimbolStack(buf, len(buf))
	return GetItemMap(buf, lis)
}
