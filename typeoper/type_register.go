package typeoper

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type TagInfo struct {
	TagName   string
	ormcolumn string
	field     reflect.StructField
	ElemKind  reflect.Kind
	dstKind   reflect.Kind
	index     int

	ormIsAutoincrement int
	ormIsPrimary       int
}

func (t *TagInfo) AllocElem(tag string, ktype reflect.Kind) {
	t.TagName = tag
}

type StructTag struct {
	tags      map[string](*TagInfo)
	IndexTags [](*TagInfo)
	stname    string
	desc      string

	OrmPrimaryIndex []int
	OrmPrimaryKeys  []string
}

//递归清空结构体内容
func ClearStruct(val reflect.Value, ktype reflect.Type) int {
	for {
		if (ktype).Kind() == reflect.Ptr {
			val = val.Elem()
			ktype = val.Type()
		} else {
			break
		}
	}

	/*	if ktype.Kind() != reflect.Struct && ktype.Kind() != reflect.Slice {
		val.Set(reflect.Zero(ktype))
	}*/
	kkind := ktype.Kind()
	if kkind == reflect.Slice {
		len := val.Len()
		//fmt.Printf("clear val %+v %+v\n", val, len)
		for i := 0; i < len; i++ {
			ClearStruct(val.Index(i), val.Index(i).Type())
		}
		if val.Cap() > 0 {
			val.SetLen(0)
		}
		return 0
	}

	if kkind == reflect.Struct {
		field_num := ktype.NumField()
		for i := 0; i < field_num; i++ {
			v := val.Field(i)
			t := v.Type()
			for {
				if t.Kind() == reflect.Ptr {
					v = v.Elem()
					t = v.Type()
				} else {
					break
				}
			}
			tkind := t.Kind()
			if tkind == reflect.Slice {
				if v.Len() > 0 {
					va := v.Index(0)
					for {
						if va.Type().Kind() == reflect.Ptr {
							va = va.Elem()
						} else {
							break
						}
					}
					if va.Type().Kind() != reflect.Struct && va.Type().Kind() != reflect.Slice {
						if v.Cap() > 0 {
							v.SetLen(0)
							v.SetCap(0)
						}
						return 0
					}
				} else {
					return 0
				}
			}

			if tkind == reflect.Struct {
				ClearStruct(v, v.Type())
				return 0
			}
			if tkind == reflect.Slice {
				/*if v.Cap() > 0 {
					ClearStruct(v, v.Type())
				}*/
				v.SetLen(0)
				v.SetCap(0)
				return 0
			}
			//v.Set(reflect.Zero(t))
			switch tkind {
			case reflect.String:
				v.SetString("")
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v.SetInt(0)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				v.SetUint(0)
			case reflect.Float32, reflect.Float64:
				v.SetFloat(0)
			default:
				v.Set(reflect.Zero(t))
			}
		}
	}
	return 0
}

func (t *StructTag) RegisteStruct(ktype reflect.Type, desc string) int {
	field_num := ktype.NumField()
	t.desc = desc

	t.IndexTags = make([](*TagInfo), field_num)
	for i := 0; i < field_num; i++ {
		field := ktype.Field(i)
		strtag := field.Tag.Get(desc)
		if strtag == "" || strtag == "-" {
			continue
		}

		var tag TagInfo
		tag.field = field
		tag.TagName = strtag
		tag.index = i
		if field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Array {
			tag.ElemKind = field.Type.Elem().Kind()
		} else {
			tag.ElemKind = field.Type.Kind()
		}

		tag.dstKind = tag.ElemKind
		if field.Type.Kind() == reflect.Ptr {
			tag.dstKind = field.Type.Elem().Kind()
		}
		t.tags[strtag] = &tag
		t.IndexTags[i] = &tag
	}
	return 0
}

func (t *StructTag) getOrmInfo() int {
	for i := 0; i < len(t.IndexTags); i++ {
		if t.IndexTags[i] != nil {
			arrstr := strings.Split(t.IndexTags[i].TagName, ";")
			realkey := t.IndexTags[i].TagName
			for j := 0; j < len(arrstr); j++ {
				if arrstr[j] == "auto_increment" {
					t.IndexTags[i].ormIsAutoincrement = 1
					continue
				}
				if arrstr[j] == "primary" {
					t.IndexTags[i].ormIsPrimary = 1
					continue
				}
				cindex := strings.LastIndex(arrstr[j], "column:")
				if cindex >= 0 {
					rs := []rune(arrstr[j])
					realkey = string(rs[cindex+len("column:"):])
				}
			}
			if t.IndexTags[i].ormIsPrimary == 1 {
				t.OrmPrimaryKeys = append(t.OrmPrimaryKeys, realkey)
				t.OrmPrimaryIndex = append(t.OrmPrimaryIndex, i)
			}

			t.tags[realkey] = t.IndexTags[i]
			t.IndexTags[i].ormcolumn = realkey
		}
	}
	return 0
}

func (t *StructTag) Init(stname string, desc string) {
	t.stname = stname
	t.desc = desc
	t.tags = make(map[string](*TagInfo))
}

func (t *StructTag) GetStructElemKind(stentry *reflect.Value, strTag string) reflect.Kind {
	taginfo, err := t.tags[strTag]
	if err == false {
		return reflect.Invalid
	}
	return taginfo.dstKind
}

//获取可赋值的类型，包括struct array slice
func (t *StructTag) GetStructElem(stentry *reflect.Value, strTag string) (*reflect.Value, *reflect.Type) {
	taginfo, ok := t.tags[strTag]
	if ok == false {
		return nil, nil
	}

	rkind := taginfo.field.Type.Kind()
	if rkind == reflect.Ptr {
		rkind = taginfo.field.Type.Elem().Kind()
	}

	if rkind != reflect.Struct && rkind != reflect.Array && rkind != reflect.Slice {
		//return nil, nil
		val := (*stentry).Field(taginfo.index)
		if taginfo.field.Type.Kind() == reflect.Ptr {
			vtype := taginfo.field.Type.Elem()
			return &val, &vtype
		} else {
			vtype := taginfo.field.Type
			return &val, &vtype
		}
	}

	if taginfo.field.Type.Kind() == reflect.Ptr {
		var stvalue reflect.Value
		stvalue = reflect.New(taginfo.field.Type.Elem())
		if (*stentry).Type().Kind() == reflect.Ptr {
			(*stentry).Elem().Field(taginfo.index).Set(reflect.Indirect(stvalue).Addr())
		} else {
			(*stentry).Field(taginfo.index).Set(reflect.Indirect(stvalue).Addr())
		}
		t := taginfo.field.Type.Elem()
		return &stvalue, &t
	} else {
		var stvalue reflect.Value
		if (*stentry).Type().Kind() == reflect.Ptr {
			stvalue = (*stentry).Elem().Field(taginfo.index)
		} else {
			stvalue = (*stentry).Field(taginfo.index)
		}
		return &stvalue, &taginfo.field.Type
	}
	//	fmt.Printf("stvalue %+v\n", stvalue)
	//return stvalue*/
}

//根据类型，生成单个元素 for slice
func (t *StructTag) AllocSliceElem(stentry *reflect.Value) (*reflect.Value, *reflect.Type) {
	if stentry.Type().Kind() != reflect.Slice {
		return nil, nil
	}
	vtype := stentry.Type().Elem()
	var v reflect.Value

	if stentry.Cap() == 0 {
		varr := reflect.MakeSlice(stentry.Type(), 0, 4)
		stentry.Set(varr)
	} else {
		if stentry.Cap() > stentry.Len()+1 {
		} else {
			varr := reflect.MakeSlice(stentry.Type(), stentry.Len(), stentry.Len()*2)
			reflect.Copy(varr, *stentry)
			(*stentry).Set(varr)
		}

	}
	stentry.SetLen(stentry.Len() + 1)
	v = ((*stentry).Index(stentry.Len() - 1))

	if vtype.Kind() == reflect.Ptr {
		vt := reflect.New(vtype.Elem())
		v.Set(reflect.Indirect(vt).Addr())
		v = vt
		vtype = vtype.Elem()
	}

	return &v, &vtype
}

//set basic type value
func (t *StructTag) SetElemValue(stentry *reflect.Value, strTag string, strValue string) error {
	taginfo, err := t.tags[strTag]
	if err == false {
		return nil
	}
	ktype := taginfo.field.Type.Kind()
	var stvalue reflect.Value
	if stentry.Type().Kind() != reflect.Struct {
		stvalue = stentry.Elem().Field(taginfo.index)
	} else {
		stvalue = stentry.Field(taginfo.index)
	}

	if taginfo.field.Type.Kind() == reflect.Ptr {
		ktype = taginfo.dstKind
		st_rvalue := reflect.New(taginfo.field.Type.Elem())
		stvalue.Set(reflect.Indirect(st_rvalue).Addr())

		stvalue = reflect.Indirect(st_rvalue)
	}

	var rType = stvalue.Type()
	switch ktype {
	case reflect.String:
		stvalue.SetString(string(strValue))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil || reflect.Zero(rType).OverflowInt(n) {
			return errors.New("int over flow" + taginfo.field.Type.Name())
		}
		stvalue.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(strValue, 10, 64)
		if err != nil || reflect.Zero(rType).OverflowUint(n) {
			return errors.New("int over flow" + taginfo.field.Type.Name())
		}
		stvalue.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(strValue, taginfo.field.Type.Bits())
		if err != nil || reflect.Zero(rType).OverflowFloat(n) {
			return errors.New("int over float flow" + taginfo.field.Type.Name())
		}
		stvalue.SetFloat(n)
	case reflect.Slice:
		if taginfo.field.Type.Elem().Kind() == reflect.Uint8 {
			stvalue.SetBytes([]byte(strValue))
		} else {
			return errors.New("not basic elem")
		}
	}
	return nil
}

func (t *StructTag) GetBasicValue(stvalue *reflect.Value, kind reflect.Kind) (string, error) {
	ival := reflect.Indirect(*stvalue)
	switch kind {
	case reflect.Slice:
		if stvalue.Type().Elem().Kind() == reflect.Uint8 {
			return string(ival.Bytes()), nil
		} else {
			return "", errors.New("not basic elem")
		}
	case reflect.String:
		if ival.Kind() == reflect.String {
			return ival.String(), nil
		} else if ival.Kind() == reflect.Slice || ival.Kind() == reflect.Array {
			return string(ival.Bytes()), nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(ival.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(ival.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		var bstr string
		if kind == reflect.Float32 {
			bstr = strconv.FormatFloat(ival.Float(), 'G', -1, 32)
		} else {
			bstr = strconv.FormatFloat(ival.Float(), 'G', -1, 64)
		}

		return bstr, nil
	default:
		return "", errors.New("not basic elem")
	}
	return "", nil
}

func (t *StructTag) AppendBasicArrayElem(stentry *reflect.Value, str string) error {
	if stentry.Type().Kind() != reflect.Slice {
		return errors.New("type error not slice")
	}
	if stentry.Cap() == 0 {
		varr := reflect.MakeSlice(stentry.Type(), 0, 8)
		stentry.Set(varr)
	}
	if stentry.Cap() < stentry.Len()+1 {
		av := reflect.MakeSlice(stentry.Type(), stentry.Len(), stentry.Len()*2)
		reflect.Copy(av, *stentry)
		stentry.Set(av)
	}
	valkind := stentry.Type().Elem().Kind()
	valtype := stentry.Type().Elem()
	stentry.SetLen(stentry.Len() + 1)
	kv := stentry.Index(stentry.Len() - 1)
	switch valkind {
	case reflect.String:
		kv.SetString(str)
	case reflect.Slice:
		if valtype.Elem().Kind() == reflect.Uint8 {
			kv.SetBytes([]byte(str))
		} else {
			return errors.New("type error, not bytes array")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(str, 10, 64)
		if err != nil || reflect.Zero(valtype).OverflowInt(n) {
			return errors.New("int over flow" + stentry.Type().Name())
		}
		kv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(str, 10, 64)
		if err != nil || reflect.Zero(valtype).OverflowUint(n) {
			return errors.New("int over flow" + stentry.Type().Name())
		}
		kv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(str, valtype.Bits())
		if err != nil || reflect.Zero(valtype).OverflowFloat(n) {
			return errors.New("int over float flow" + stentry.Type().Name())
		}
		kv.SetFloat(n)
	default:
		return errors.New("type error, not basic type")
	}
	return nil
}

//tag 映射器，维护全局结构体tag到字段的映射，避免遍历
type StructCollect struct {
	collect map[reflect.Type](*StructTag)
	desc    string
	rwlock  sync.RWMutex
}

func (t *StructCollect) Init(desc string) {
	t.collect = make(map[reflect.Type](*StructTag))
	t.desc = desc
}

func (t *StructCollect) registStruct(ktype reflect.Type, desc string) *StructTag {
	name := ktype.Name()
	var structtag StructTag
	if desc != t.desc {
		return nil
	}
	structtag.Init(name, desc)
	structtag.RegisteStruct(ktype, desc)
	t.collect[ktype] = &structtag

	return &structtag
}

//根据reflect type，获取tag映射表
func (t *StructCollect) GetStructTagInfo(sttype *reflect.Type) *StructTag {
	if sttype == nil {
		return nil
	}

	tType := *sttype
	for tType.Kind() == reflect.Ptr {
		tType = tType.Elem()
	}
	if tType.Kind() != reflect.Struct {
		return nil
	}
	t.rwlock.RLock()
	sttags, ok := t.collect[tType]
	t.rwlock.RUnlock()
	if ok != true {
		t.rwlock.Lock()
		sttags = t.registStruct(tType, t.desc)
		sttags.getOrmInfo()
		//sttags, ok = t.collect[*sttype]
		t.rwlock.Unlock()
	}
	return sttags
}

func (t *StructCollect) GetOrmStructTagInfo(sttype *reflect.Type) *StructTag {
	return t.GetStructTagInfo(sttype)
}

func (t *StructCollect) GetOrmElem(sttags *StructTag, entry *reflect.Value, arrtags []string) interface{} {
	var ktype *reflect.Type
	for {
		if (*entry).Kind() == reflect.Ptr {
			v := (*entry).Elem()
			entry = &v
		} else {
			break
		}
	}
	for i := 0; i < len(arrtags)-1; i++ {
		entry, ktype = sttags.GetStructElem(entry, arrtags[i])
		sttags = t.GetOrmStructTagInfo(ktype)
	}
	tag := arrtags[len(arrtags)-1]
	taginfo := sttags.tags[tag]
	if taginfo == nil {
		return nil
	}
	//stvalue := (*entry).Field(taginfo.index)
	return reflect.Indirect((*entry).Field(taginfo.index)).Addr().Interface()
}

func (t *StructCollect) HasElem(sttags *StructTag, arrkey []string) bool {
	for i := 0; i < len(arrkey); i++ {
		keyinfo, ok := sttags.tags[arrkey[i]]
		if ok == false {
			return false
		}
		if i < len(arrkey)-1 {
			sttags = t.GetOrmStructTagInfo(&keyinfo.field.Type)
		}
	}
	return true
}

func (t *StructCollect) GetAllOrmElems(sttags *StructTag, kheader string, asheader string, assep string) []string {
	var arrkeys []string
	for i := 0; i < len(sttags.IndexTags); i++ {
		if sttags.IndexTags[i] == nil {
			continue
		}
		//arrkey := kheader + "." + sttags.IndexTags[i].ormcolumn
		if sttags.IndexTags[i].field.Type.Kind() == reflect.Struct {
			if kheader != "" {
				kheader = kheader + "." + sttags.IndexTags[i].ormcolumn
				asheader = asheader + assep + sttags.IndexTags[i].ormcolumn
			} else {
				kheader = sttags.IndexTags[i].ormcolumn
				asheader = sttags.IndexTags[i].ormcolumn
			}
			sttags = t.GetOrmStructTagInfo(&sttags.IndexTags[i].field.Type)
			arrkeys = append(arrkeys, t.GetAllOrmElems(sttags, kheader, asheader, assep)...)
		} else {
			var key string
			if kheader != "" {
				key = kheader + "." + sttags.IndexTags[i].ormcolumn + " as " + asheader + assep + sttags.IndexTags[i].ormcolumn
			} else {
				key = sttags.IndexTags[i].ormcolumn
			}
			arrkeys = append(arrkeys, key)
		}
	}
	return arrkeys
}

func (t *StructCollect) GetOrmInsertInfo(val reflect.Value, onDuplicate int) (string, []interface{}, *reflect.Value) {
	keys, vals, priv := t.getOrmInsertInfo(val, true, true, true, onDuplicate)
	return strings.Join(keys, ","), vals, priv
}

func (t *StructCollect) GetOrmInsertKeys(val reflect.Value, onDuplicate int) []string {
	keys, _, _ := t.getOrmInsertInfo(val, true, false, false, onDuplicate)
	return keys
}

func (t *StructCollect) GetOrmInsertVals(val reflect.Value, onDuplicate int) []interface{} {
	_, vals, _ := t.getOrmInsertInfo(val, false, true, false, onDuplicate)
	return vals
}

func (t *StructCollect) getOrmInsertInfo(val reflect.Value, needKey bool, needVal bool, needAutoVal bool, onDuplicate int) ([]string, []interface{}, *reflect.Value) {
	var vtype = val.Type()
	var sttags = t.GetOrmStructTagInfo(&vtype)
	var paras []interface{}
	var simbols []string
	var rpk *reflect.Value
	for i := 0; i < len(sttags.IndexTags); i++ {
		if sttags.IndexTags[i] == nil {
			continue
		}
		if sttags.IndexTags[i].field.Type.Kind() == reflect.Struct {
			continue
		}
		if onDuplicate == 0 {
			if sttags.IndexTags[i].ormIsAutoincrement == 1 {
				rpkval := val.Field(i)
				rpk = &rpkval
				continue
			}
		}

		vif := reflect.Indirect(val.Field(i)).Addr().Interface()
		if needKey == true {
			simbols = append(simbols, sttags.IndexTags[i].ormcolumn)
		}
		if needVal == true {
			paras = append(paras, vif)
		}
	}

	return simbols, paras, rpk
}

const (
	ORM_FOR_VALS      = 1
	ORM_FOR_DEF       = 2
	ORM_FOR_DEF_NOPRI = 3
)

func (t *StructCollect) GetOrmKeyInfo(sttags *StructTag, val reflect.Value, usage int, keys ...string) []string {
	rkeys, _ := t.getOrmKeyValInfo(sttags, val, true, false, usage, keys...)
	return rkeys
}

func (t *StructCollect) GetOrmValInfo(sttags *StructTag, val reflect.Value, usage int, keys ...string) []interface{} {
	_, vals := t.getOrmKeyValInfo(sttags, val, false, true, usage, keys...)
	return vals
}

func (t *StructCollect) GetOrmKeyValInfo(sttags *StructTag, val reflect.Value, usage int, keys ...string) ([]string, []interface{}) {
	return t.getOrmKeyValInfo(sttags, val, true, true, usage, keys...)
}

func (t *StructCollect) getOrmKeyValInfo(sttags *StructTag, val reflect.Value, needKey bool, needVal bool, usage int, keys ...string) ([]string, []interface{}) {
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if usage == ORM_FOR_DEF {
		if len(keys) == 0 && len(sttags.OrmPrimaryIndex) == 0 {
			return nil, nil
		}
	} else if usage == ORM_FOR_VALS {
		if len(keys) == 0 {
			return nil, nil
		}
	} else {
	}

	var defkeys []string
	var defvals []interface{}
	if usage == ORM_FOR_DEF && len(sttags.OrmPrimaryIndex) > 0 {
		if needVal == true {
			for _, i := range sttags.OrmPrimaryIndex {
				vif := reflect.Indirect(val.Field(i)).Addr().Interface()
				defvals = append(defvals, vif)
			}
		}
		if needKey == true {
			defkeys = append(defkeys, sttags.OrmPrimaryKeys...)
		}
	}

	if needVal == true {
		for i := 0; i < len(keys); i++ {
			irv := t.GetOrmElem(sttags, &val, []string{keys[i]})
			if irv == nil {
				panic("failed to get key " + keys[i])
			}
			defvals = append(defvals, irv)
			irv = nil
		}
	}

	if needKey == true {
		defkeys = append(defkeys, keys...)
	}

	return defkeys, defvals
}
