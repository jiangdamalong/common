package stsql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jiangdamalong/common"
	"github.com/jiangdamalong/common/log"
	"github.com/jiangdamalong/common/typeoper"
)

const (
	SQL_IN_SELECT = 1
	SQL_IN_FROM   = 2
	SQL_IN_WHERE  = 3
	SQL_IN_OR     = 4
	SQL_IN_EXT    = 5

	SQL_IN_SET        = 6
	SQL_IN_SET_WHERE  = 7
	SQL_IN_FOR_UPDATE = 8

	SQL_FOR_COUNT           = 108
	SQL_FOR_SELECT          = 109
	SQL_FOR_INSERT          = 110
	SQL_FOR_INSERT_DUPLICAT = 111
	SQL_FOR_UPDATE          = 120
	SQL_FOR_DELETE          = 130
	SQL_FOR_SELECTUPDATE    = 140

	SQL_WHERE_NO_ORMPRI     = "$$ORM_WHERE_NO_PRI$$"
	SQL_IGNOR_GET_AUTOINCRE = "$$IGNORE_AUTO_INCRE$$"
)

var usageDef = map[int]string{
	SQL_FOR_SELECT:          "select",
	SQL_FOR_INSERT:          "insert",
	SQL_FOR_DELETE:          "delete",
	SQL_FOR_SELECTUPDATE:    "select_for_update",
	SQL_FOR_INSERT_DUPLICAT: "insert_on_duplicate",
	SQL_FOR_COUNT:           "select_count",
}

var DebugLog = false

type SqlOptions struct {
	addr        string
	user        string
	passwd      string
	db          string
	rTimeOut    int
	wTimeOut    int
	dTimeOut    int
	idleTimeOut int

	desc           string
	maxFreeConnNum int
	maxConnNum     int
}

var optionsDefault = SqlOptions{
	addr:           "",
	user:           "",
	passwd:         "",
	db:             "",
	rTimeOut:       30,
	wTimeOut:       10,
	dTimeOut:       10,
	idleTimeOut:    60 * 10,
	desc:           "gorm",
	maxFreeConnNum: 10,
	maxConnNum:     50,
}

type SqlDao struct {
	sync.Mutex
	conn    *sql.DB
	collect typeoper.StructCollect
	lock    sync.Mutex
	options SqlOptions

	freeQuery *SqlQuery
}

func (sd *SqlDao) Conn() *sql.DB {
	return sd.conn
}

func (sd *SqlDao) Init(addr string, user string, passwd string, db string) {
	sd.options = optionsDefault
	sd.options.addr = addr
	sd.options.user = user
	sd.options.passwd = passwd
	sd.options.db = db
	sd.freeQuery = nil
}

func (sd *SqlDao) SetTimeOut(rTimeOut int, wTimeOut int, dTimeOut int) {
	sd.options.rTimeOut = rTimeOut
	sd.options.wTimeOut = wTimeOut
	sd.options.dTimeOut = dTimeOut
}

func (sd *SqlDao) SetIdleTimeOut(idleMax int) {
	sd.options.idleTimeOut = idleMax
}

func (sd *SqlDao) SetConnNum(maxFreeConnNum int, maxConnNum int) {
	sd.options.maxFreeConnNum = maxFreeConnNum
	sd.options.maxConnNum = maxConnNum
}

func (sd *SqlDao) Open() error {
	connString := fmt.Sprintf("%s:%s@tcp(%s)/%s?timeout=%ds&writeTimeout=%ds&readTimeout=%ds",
		sd.options.user, sd.options.passwd, sd.options.addr, sd.options.db, sd.options.dTimeOut, sd.options.wTimeOut, sd.options.rTimeOut)
	dbconn, connerr := sql.Open("mysql", connString)
	if connerr != nil {
		return connerr
	}
	sd.collect.Init(sd.options.desc)
	sd.conn = dbconn
	sd.conn.SetMaxIdleConns(sd.options.maxFreeConnNum)
	sd.conn.SetMaxOpenConns(sd.options.maxConnNum)
	sd.conn.SetConnMaxLifetime(time.Duration(sd.options.idleTimeOut) * time.Second)
	return nil
}

func (sd *SqlDao) Close() {
	if sd.conn != nil {
		sd.conn.Close()
	}
}

func (sd *SqlDao) InitOpen(addr string, user string, passwd string, db string) error {
	sd.Init(addr, user, passwd, db)
	return sd.Open()
}

func (sd *SqlDao) FetchQuery() *SqlQuery {
	sd.Lock()
	defer sd.Unlock()
	var query *SqlQuery
	if sd.freeQuery == nil {
		query = new(SqlQuery)
	} else {
		query = sd.freeQuery
		sd.freeQuery = sd.freeQuery.next
	}

	query.Init(sd, "$_$")
	return query
}

func (sd *SqlDao) FetchQuerySpan(span common.Span) *SqlQuery {
	query := new(SqlQuery)
	query.Init(sd, "$_$")
	query.span = span.ChildSpan("DbQuery")
	return query
}

func (query *SqlQuery) SetSpan(span common.Span) {
	query.pSpan = span
	//span.ChildSpan("DbQuery", &query.span)
}

type DbStr string

type SqlQuery struct {
	sql        string
	selectKeys []string
	selectvals []string

	dstVals []interface{}
	vType   reflect.Type
	stTags  *typeoper.StructTag
	dao     *SqlDao
	begin   time.Time

	updateKeys []string

	condVal  []interface{}
	condNums []int
	condNum  int

	slConditions string
	slVals       []interface{}

	sqls []string

	status int
	usage  int

	sep string

	count      int
	rowsAffect int
	err        error

	pSpan common.Span
	span  common.Span

	next *SqlQuery
}

func (sq *SqlQuery) Init(dao *SqlDao, sep string) {
	sq.reset()
	sq.dao = dao
	sq.sep = sep
	sq.begin = time.Now()
}

func (sq *SqlQuery) setType(vType reflect.Type) {
	for vType.Kind() == reflect.Ptr {
		vType = vType.Elem()
	}
	if vType.Kind() != reflect.Struct {
		panic("just support struct type")
	}
	sq.vType = vType
	sq.stTags = sq.dao.collect.GetOrmStructTagInfo(&sq.vType)
}

func (sq *SqlQuery) reset() {
	clearVals(sq.dstVals)
	sq.dstVals = sq.dstVals[0:0]
	sq.selectKeys = sq.selectKeys[0:0]
	clearVals(sq.condVal)
	sq.condVal = sq.condVal[0:0]
	sq.selectvals = sq.selectvals[0:0]
	sq.sql = ""
	sq.status = 0
	sq.count = 0
	sq.err = nil
	sq.usage = 0
	sq.condNum = 0
	sq.condNums = sq.condNums[0:0]
	sq.slConditions = ""
	clearVals(sq.slVals)
	sq.slVals = sq.slVals[0:0]
	sq.stTags = nil
	sq.updateKeys = sq.updateKeys[0:0]
	sq.sqls = sq.sqls[0:0]
	sq.rowsAffect = 0
	sq.span = nil
}

func (sq *SqlQuery) clearPara() {
	sq.selectKeys = sq.selectKeys[0:0]
	clearVals(sq.condVal)
	sq.condVal = sq.condVal[0:0]
	sq.selectvals = sq.selectvals[0:0]
	sq.sql = ""
	sq.status = 0
	sq.count = 0
	sq.err = nil
	sq.usage = 0
	sq.condNum = 0
	sq.condNums = sq.condNums[0:0]
	sq.slConditions = ""
	clearVals(sq.slVals)
	sq.slVals = sq.slVals[0:0]
	sq.stTags = nil
	sq.updateKeys = sq.updateKeys[0:0]
	sq.sqls = sq.sqls[0:0]
	sq.rowsAffect = 0
	sq.span = nil
}

func clearVals(vals []interface{}) {
	for i := 0; i < len(vals); i++ {
		vals[i] = nil
	}
}

func (sq *SqlQuery) insert(table string, rval reflect.Value) error {
	sq.sql = "insert into " + table
	sq.usage = SQL_FOR_INSERT
	for rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}

	strsims, vals, rkey := sq.dao.collect.GetOrmInsertInfo(rval, 0)
	defer clearVals(vals)
	if vals == nil {
		panic("invalid struct")
	}
	sq.condVal = append(sq.condVal, vals...)
	sq.sql = sq.sql + "(" + strsims + ") values(" + strings.Repeat("?,", len(vals)-1) + "?)"

	var res sql.Result
	res, sq.err = sq.dao.conn.Exec(sq.sql, sq.condVal...)
	if sq.err != nil {
		return sq.err
	}
	if rkey != nil {
		var aid int64
		aid, sq.err = res.LastInsertId()
		if sq.err == nil {
			(*rkey).SetInt(aid)
		}
		var arrows int64
		arrows, sq.err = res.RowsAffected()
		if sq.err != nil {
			return sq.err
		} else {
			sq.rowsAffect += (int)(arrows)
		}
	}
	sq.err = nil
	return nil
}

func (sq *SqlQuery) Insert(table string, val interface{}) error {
	defer func() {
		sq.Report()
	}()

	sq.reset()
	rval := reflect.ValueOf(val)
	for rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	if rval.Kind() == reflect.Struct {
		return sq.insert(table, reflect.ValueOf(val))
	} else {
		_, err := sq.minsert(table, true, val)
		return err
	}
}

func (sq *SqlQuery) minsert(table string, needAuto bool, val interface{}) (int, error) {
	sq.reset()
	var aval = reflect.ValueOf(val)
	for aval.Kind() == reflect.Ptr {
		aval = aval.Elem()
	}

	if aval.Kind() == reflect.Struct {
		return 0, sq.insert(table, aval)
	}

	if aval.Kind() != reflect.Array && aval.Kind() != reflect.Slice {
		panic("invalid data type")
	}

	if aval.Len() == 0 {
		return 0, errors.New("no valid input struct")
	}
	if needAuto == true {
		for i := 0; i < aval.Len(); i++ {
			v := aval.Index(i)
			err := sq.insert(table, v)
			if err != nil {
				return i, errors.New(fmt.Sprintf("%+v index %d", err, i))
			}
		}
		return aval.Len(), nil
	}

	sq.sql = "insert into " + table

	sq.usage = SQL_FOR_INSERT
	var rval reflect.Value
	rval = aval.Index(0)

	for rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}

	keys := sq.dao.collect.GetOrmInsertKeys(rval, 0)
	sq.sql += "(" + strings.Join(keys, ",") + ") values"
	simbols := "(" + strings.Repeat("?,", len(keys)-1) + "?)"
	for i := 0; i < aval.Len(); i++ {
		rval = aval.Index(i)
		for rval.Kind() == reflect.Ptr {
			rval = rval.Elem()
		}
		vals := sq.dao.collect.GetOrmInsertVals(rval, 0)
		defer clearVals(vals)
		sq.sql += simbols
		if i < aval.Len()-1 {
			sq.sql += ","
		}
		sq.condVal = append(sq.condVal, vals...)
	}

	var res sql.Result
	res, sq.err = sq.dao.conn.Exec(sq.sql, sq.condVal...)
	if sq.err != nil {
		return 0, sq.err
	}
	var arrows int64
	arrows, sq.err = res.RowsAffected()
	if sq.err == nil {
		sq.rowsAffect += (int)(arrows)
	}

	return 0, nil
}

func (sq *SqlQuery) InsertDuplicate(table string, val interface{}, keys ...interface{}) error {
	defer func() {
		sq.Report()
	}()
	sq.reset()
	rval := reflect.ValueOf(val)
	for rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	needAutoIncre := true
	if len(keys) > 0 {
		if keys[0] == SQL_IGNOR_GET_AUTOINCRE {
			needAutoIncre = false
			keys = keys[1:]
		}
	}

	if rval.Kind() == reflect.Struct {
		return sq.insertDuplicate(table, rval, keys...)
	}
	if rval.Kind() == reflect.Slice || rval.Kind() == reflect.Array {
		if rval.Len() == 0 {
			return errors.New("get zero value")
		}
		if needAutoIncre == true {
			for i := 0; i < rval.Len(); i++ {
				sq.insertDuplicate(table, rval.Index(i), keys...)
			}
		} else {
			sq.reset()
			sq.sql = "insert into " + table
			sq.usage = SQL_FOR_INSERT
			dkeys := sq.dao.collect.GetOrmInsertKeys(rval.Index(0), 1)
			sq.sql += "(" + strings.Join(dkeys, ",") + ") values "
			simbols := "(" + strings.Repeat("?,", len(dkeys)-1) + "?" + ")"
			for i := 0; i < rval.Len(); i++ {
				sq.sql += simbols
				if i < rval.Len()-1 {
					sq.sql += ","
				}
				irval := rval.Index(i)
				for irval.Kind() == reflect.Ptr {
					irval = irval.Elem()
				}
				dvals := sq.dao.collect.GetOrmInsertVals(irval, 1)
				defer clearVals(dvals)
				sq.condVal = append(sq.condVal, dvals...)
			}

			if len(keys) == 0 {
				skeys := sq.AllColumn(rval)
				keys = make([]interface{}, len(skeys))
				for i, v := range skeys {
					keys[i] = v
				}
			}
			sq.sql += " ON DUPLICATE KEY UPDATE "
			for i, k := range keys {
				if reflect.TypeOf(k).Name() == "DbStr" {
					sq.sql += (string(k.(DbStr)))
				} else {
					sq.sql += k.(string) + "=values(" + k.(string) + ")"
				}
				if i < len(keys)-1 {
					sq.sql += ","
				}
			}
			var res sql.Result
			res, sq.err = sq.dao.conn.Exec(sq.sql, sq.condVal...)
			if sq.err != nil {
				return sq.err
			}
			ret, err := res.RowsAffected()
			if err == nil {
				sq.rowsAffect = (int)(ret)
			}
			return nil
		}
	} else {
		panic("get invalid type")
	}
	return nil
}

func (sq *SqlQuery) insertDuplicate(table string, rval reflect.Value, keys ...interface{}) error {
	/*if len(keys) == 0 {
		return errors.New("get empty keys for insert on duplicate")
	}*/
	sq.sql = "insert into " + table
	sq.usage = SQL_FOR_INSERT
	for rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}

	if rval.Kind() == reflect.Struct {
		strsims, vals, rkey := sq.dao.collect.GetOrmInsertInfo(rval, 1)
		defer clearVals(vals)

		if vals == nil {
			panic("invalid struct")
		}
		sq.condVal = vals
		sq.sql = sq.sql + "(" + strsims + ") values(" + strings.Repeat("?,", len(vals)-1) + "?)"
		sq.sql += " ON DUPLICATE KEY UPDATE "

		if len(keys) == 0 {
			skeys := sq.AllColumn(rval)
			keys = make([]interface{}, len(skeys))
			for i, v := range skeys {
				keys[i] = v
			}
		}

		for i, k := range keys {
			if reflect.TypeOf(k).Name() == "DbStr" {
				sq.sql += (string(k.(DbStr)))
			} else {
				sq.sql += k.(string) + "=values(" + k.(string) + ")"
			}
			if i < len(keys)-1 {
				sq.sql += ","
			}
		}

		var res sql.Result
		res, sq.err = sq.dao.conn.Exec(sq.sql, sq.condVal...)
		if sq.err != nil {
			return sq.err
		}
		if rkey != nil {
			var pkey int64
			pkey, sq.err = res.LastInsertId()
			if sq.err == nil {
				(*rkey).SetInt(pkey)
			}
		}
		var arrows int64
		arrows, sq.err = res.RowsAffected()
		if sq.err != nil {
			return sq.err
		} else {
			sq.rowsAffect += (int)(arrows)
		}
	} else {
		panic("invalid data type not struct")
	}
	return nil
}

func (sq *SqlQuery) SelectAll(vType reflect.Type) *SqlQuery {
	sq.reset()
	sq.setType(vType)
	var vals = sq.dao.collect.GetAllOrmElems(sq.stTags, "", "", sq.sep)
	sq.status = SQL_IN_SELECT
	sq.usage = SQL_FOR_SELECT
	sq.sql = "select "
	for i := 0; i < len(vals); i++ {
		key := vals[i]

		rs := []rune(vals[i])
		pos := strings.LastIndex(vals[i], " as ")
		if pos > 0 {
			key = string(rs[pos+len(" as "):])
		}
		sq.selectKeys = append(sq.selectKeys, key)
	}
	sjoin := strings.Join(vals, ",")
	sq.sql = sq.sql + sjoin + " "
	return sq
}

func (sq *SqlQuery) Select(vType reflect.Type, vals ...string) *SqlQuery {
	if len(vals) == 0 {
		return sq.SelectAll(vType)
	}
	sq.reset()
	sq.setType(vType)
	sq.status = SQL_IN_SELECT
	sq.usage = SQL_FOR_SELECT
	sq.sql = "select "
	for i := 0; i < len(vals); i++ {
		key := vals[i]

		rs := []rune(vals[i])
		pos := strings.LastIndex(vals[i], " as ")
		if pos > 0 {
			key = string(rs[pos+len(" as "):])
		}

		if sq.dao.collect.HasElem(sq.stTags, strings.Split(key, sq.sep)) {
			sq.selectKeys = append(sq.selectKeys, key)
		} else {
			panic("key not exist in struct " + vals[i] + " rk:" + key)
		}
	}
	sq.selectvals = vals
	sjoin := strings.Join(vals, ",")
	sq.sql = sq.sql + sjoin + " "
	return sq
}

func (sq *SqlQuery) SelectForUpdate(keys ...string) *SqlQuery {
	sq.reset()
	sq.status = SQL_IN_SELECT
	sq.usage = SQL_FOR_SELECTUPDATE
	sq.sql = "select "
	sq.sql = sq.sql + strings.Join(keys, ",")
	sq.updateKeys = keys
	return sq
}

func (sq *SqlQuery) SelectCount(vars ...string) *SqlQuery {
	sq.reset()
	sq.status = SQL_IN_SELECT
	sq.usage = SQL_FOR_COUNT
	if len(vars) == 0 {
		sq.sql = "select count(*) "
	} else {
		sq.sql = "select count(" + strings.Join(vars, ",") + ")"
	}
	return sq
}

func (sq *SqlQuery) SelectSum(field string) *SqlQuery {
	sq.reset()
	sq.status = SQL_IN_SELECT
	sq.usage = SQL_FOR_COUNT
	sq.sql = "select sum(" + field + ")"
	return sq
}

func (sq *SqlQuery) From(tables ...string) *SqlQuery {
	sq.status = SQL_IN_FROM
	sjoin := strings.Join(tables, ",")
	sq.sql = sq.sql + " from " + sjoin + " "
	return sq
}

func (sq *SqlQuery) Where(cond ...interface{}) *SqlQuery {
	if reflect.TypeOf(cond[0]).Kind() != reflect.String {
		if len(cond) > 0 {
			var keys []string
			for _, v := range cond[1:] {
				keys = append(keys, v.(string))
			}
			return sq.SetWhere(cond[0], keys...)
		} else {
			return sq.SetWhere(cond[0])
		}
	}
	if sq.status == SQL_IN_SET && sq.usage == SQL_FOR_SELECTUPDATE {
		return sq
	}

	if sq.status == SQL_IN_WHERE || sq.status == SQL_IN_OR {
		sq.sql = sq.sql + " and ("
		sq.slConditions += " and ("
	} else {
		sq.sql = sq.sql + " where ("
		sq.slConditions += " where ("
	}
	sq.status = SQL_IN_WHERE
	if len(cond)%3 != 0 {
		return nil
	}
	var keys = make([]string, len(cond)/3)
	for i := 0; i < len(cond)/3; i++ {
		switch reflect.TypeOf(cond[i*3+2]).Name() {
		case "DbStr":
			keys[i] = cond[i*3].(string) + cond[i*3+1].(string) + string(cond[i*3+2].(DbStr))
		default:
			keys[i] = cond[i*3].(string) + cond[i*3+1].(string) + "?"
			sq.condVal = append(sq.condVal, cond[i*3+2])
			if sq.usage == SQL_FOR_SELECTUPDATE {
				sq.condNum++
				sq.slVals = append(sq.slVals, cond[i*3+2])
			}
		}
	}
	sjoin := strings.Join(keys, " and ")
	sq.sql = sq.sql + sjoin + ") "
	sq.slConditions += sjoin + ") "

	return sq
}

func (sq *SqlQuery) Or(cond ...interface{}) *SqlQuery {
	if sq.status == SQL_IN_SET && sq.usage == SQL_FOR_SELECTUPDATE {
		return sq
	}

	if sq.status == SQL_IN_WHERE || sq.status == SQL_IN_OR {
		sq.sql = sq.sql + " or ("
	} else {
		sq.sql = sq.sql + " where ("
	}
	sq.status = SQL_IN_OR
	if len(cond)%3 != 0 {
		panic("invalid para num")
	}
	var keys = make([]string, len(cond)/3)
	for i := 0; i < len(cond)/3; i++ {
		switch reflect.TypeOf(cond[i*3+2]).Name() {
		case "DbStr":
			keys[i] = cond[i*3].(string) + cond[i*3+1].(string) + string(cond[i*3+2].(DbStr))
		default:
			keys[i] = cond[i*3].(string) + cond[i*3+1].(string) + "?"
			sq.condVal = append(sq.condVal, cond[i*3+2])
			if sq.usage == SQL_FOR_SELECTUPDATE {
				sq.condNum++
				sq.slVals = append(sq.slVals, cond[i*3+2])
			}
		}
	}
	sjoin := strings.Join(keys, " or ")
	sq.sql = sq.sql + sjoin + ") "
	return sq
}

func (sq *SqlQuery) Ext(ext string) *SqlQuery {
	sq.sql = sq.sql + ext + " "
	return sq
}

func (sq *SqlQuery) execMulti() (sql.Result, error) {
	var result sql.Result
	var cerr error
	beginIndex := 0
	endIndex := 0
	for i, sql := range sq.sqls {
		endIndex = sq.condNums[i]
		if beginIndex < endIndex && endIndex > 0 {
			result, cerr = sq.dao.conn.Exec(sql, sq.condVal[beginIndex:endIndex]...)
		} else {
			result, cerr = sq.dao.conn.Exec(sql)
		}
		beginIndex = endIndex
		if cerr != nil {
			return nil, cerr
		}
	}
	return result, cerr
}

func (sq *SqlQuery) Report() {
	spend := time.Since(sq.begin)
	if sq.err != nil {
		str, _ := InterpolateParams(sq.sql, sq.condVal)
		if str == "" {
			str = sq.sql
		}
		log.StLogger.WriteLogf(log.ErrorLevel, 3, "sq error sql: %+v; error : %+v", str, sq.err)
	}

	//默认不打这条日志
	if DebugLog == true {
		str, _ := InterpolateParams(sq.sql, sq.condVal)
		if str == "" {
			str = sq.sql
		}
		log.StLogger.WriteLogf(log.InfoLevel, 3, "sq spend time sql: %+v; spend(millisecond) : %v", str, int64(spend/(time.Millisecond)))
	}

	if spend > 200*time.Millisecond {
		str, _ := InterpolateParams(sq.sql, sq.condVal)
		if str == "" {
			str = sq.sql
		}
		log.StLogger.WriteLogf(log.ErrorLevel, 3, "sq spend time sql too long: %+v; spend(millisecond) : %v", str, int64(spend/(time.Millisecond)))
	}

	if sq.pSpan != nil {
		sq.span = sq.pSpan.ChildSpan("DbQuery")
	} else {
		return
	}
	sq.span.SetTag("usage", usageDef[sq.usage])
	sq.span.SetTag("sql", sq.sql)
	if sq.err != nil {
		sq.span.SetTag("sql_err", sq.err)
	}
	sq.span.SetTag("affect", sq.rowsAffect)
	sq.span.SetTag("count", sq.count)
	sq.span.Finish()
}

func (sq *SqlQuery) Into(val interface{}) error {
	defer func() {
		sq.Report()
	}()

	if sq.status != SQL_IN_WHERE && sq.status != SQL_IN_OR && sq.status != SQL_IN_SET_WHERE {
		sq.err = errors.New("no limit set,maybe illeagle request : " + sq.sql)
		return sq.err
	}
	var rows *sql.Rows
	rows, sq.err = sq.dao.conn.Query(sq.sql, sq.condVal...)
	if sq.err != nil {
		return sq.err
	}

	erv := reflect.ValueOf(val)
	for erv.Kind() == reflect.Ptr {
		erv = erv.Elem()
	}

	for rows.Next() {
		var vif = make([]interface{}, 0, len(sq.selectKeys))
		for i := 0; i < len(sq.selectKeys); i++ {
			var arrkey = strings.Split(sq.selectKeys[i], sq.sep)
			var rv = sq.dao.collect.GetOrmElem(sq.stTags, &erv, arrkey)
			vif = append(vif, rv)
			rv = nil
		}
		rows.Scan(vif...)
		for i := 0; i < len(vif); i++ {
			vif[i] = nil
		}
		return nil
	}
	return errors.New("no data find")
}

func (sq *SqlQuery) Run() error {
	defer func() {
		sq.Report()
	}()

	if sq.status != SQL_IN_WHERE && sq.status != SQL_IN_OR && sq.status != SQL_IN_SET_WHERE {
		sq.err = errors.New("no limit set,maybe illeagle request : " + sq.sql)
		return sq.err
	}

	if sq.usage == SQL_FOR_UPDATE || sq.usage == SQL_FOR_INSERT || sq.usage == SQL_FOR_DELETE || sq.usage == SQL_FOR_SELECTUPDATE {
		var result sql.Result
		if sq.usage == SQL_FOR_SELECTUPDATE {
			sq.condNums = append(sq.condNums, sq.condNum)
			sq.sqls = append(sq.sqls, sq.sql)
			result, sq.err = sq.execMulti()
			sq.dao.conn.Exec("commit")
		} else {
			result, sq.err = sq.dao.conn.Exec(sq.sql, sq.condVal...)
		}
		if sq.err != nil {
			return sq.err
		}
		var arrows int64
		arrows, sq.err = result.RowsAffected()
		if sq.err != nil {
			return sq.err
		} else {
			sq.err = nil
			sq.rowsAffect = (int)(arrows)
			return nil
		}
	}
	var rows *sql.Rows
	rows, sq.err = sq.dao.conn.Query(sq.sql, sq.condVal...)
	if sq.err != nil {
		return sq.err
	}
	defer rows.Close()
	for rows.Next() {
		if sq.usage == SQL_FOR_COUNT {
			rows.Scan(&sq.count)
			return nil
		}
		val := reflect.New(sq.vType)
		var vif = make([]interface{}, 0, len(sq.selectKeys))
		for i := 0; i < len(sq.selectKeys); i++ {
			var arrkey = strings.Split(sq.selectKeys[i], sq.sep)
			var rv = sq.dao.collect.GetOrmElem(sq.stTags, &val, arrkey)
			vif = append(vif, rv)
			rv = nil
		}
		rows.Scan(vif...)
		sq.dstVals = append(sq.dstVals, reflect.Indirect(val).Addr().Interface())
		for i := 0; i < len(vif); i++ {
			vif[i] = nil
		}
	}
	sq.count = len(sq.dstVals)
	return nil
}

func (sq *SqlQuery) Update(table string) *SqlQuery {
	if sq.usage == SQL_FOR_SELECTUPDATE {
		if sq.status != SQL_IN_WHERE {
			panic("no where set in select update")
		}
		sq.sql += " for update"
		sq.sqls = append(sq.sqls, sq.sql)
		sq.condNums = append(sq.condNums, sq.condNum)
	} else {
		sq.reset()
	}
	sq.status = SQL_IN_SET
	if sq.usage != SQL_FOR_SELECTUPDATE {
		sq.usage = SQL_FOR_UPDATE
	}
	sq.sql = "update " + table + " set "
	return sq
}

func (sq *SqlQuery) Set(vif interface{}, keys ...string) *SqlQuery {
	if len(keys) == 0 {
		return sq.SetAll(vif)
	}
	sq.status = SQL_IN_SET
	vvalue := reflect.ValueOf(vif)
	for vvalue.Kind() == reflect.Ptr {
		vvalue = vvalue.Elem()
	}
	vType := vvalue.Type()
	stTags := sq.dao.collect.GetOrmStructTagInfo(&vType)
	keys, vals := sq.dao.collect.GetOrmKeyValInfo(stTags, vvalue, typeoper.ORM_FOR_VALS, keys...)
	defer clearVals(vals)
	if keys == nil {
		panic("failed to get orm set info")
	}

	sq.sql = sq.sql + strings.Join(keys, "=?,")
	sq.sql = sq.sql + "=?"
	sq.sql = sq.sql + " "

	sq.condVal = append(sq.condVal, vals...)

	if sq.usage == SQL_FOR_SELECTUPDATE {
		sq.condNum += len(vals)
		sq.condVal = append(sq.condVal, sq.slVals...)
		sq.condNum += len(sq.slVals)
		sq.sql += sq.slConditions
		sq.status = SQL_IN_WHERE
	}

	return sq
}

func (sq *SqlQuery) SetAll(vif interface{}) *SqlQuery {
	return sq.Set(vif, sq.AllColumn(reflect.ValueOf(vif))...)
}

func (sq *SqlQuery) AllColumn(vif reflect.Value) []string {
	vType := vif.Type()
	for vType.Kind() == reflect.Ptr {
		vType = vType.Elem()
	}

	tags := sq.dao.collect.GetStructTagInfo(&vType)
	return sq.dao.collect.GetAllOrmElems(tags, "", "", sq.sep)
}

func (sq *SqlQuery) SetWhere(vif interface{}, keys ...string) *SqlQuery {
	if sq.status == SQL_IN_SET && sq.usage == SQL_FOR_SELECTUPDATE {
		return sq
	}
	if sq.status == SQL_IN_WHERE || sq.status == SQL_IN_OR {
		sq.sql += " and ("
		sq.slConditions += " and ("
	} else {
		sq.sql += " where ("
		sq.slConditions += " where ("
	}
	sq.status = SQL_IN_SET_WHERE

	ivalue := reflect.ValueOf(vif)
	for ivalue.Kind() == reflect.Ptr {
		ivalue = ivalue.Elem()
	}

	ormGetType := typeoper.ORM_FOR_DEF
	if len(keys) > 1 && keys[0] == SQL_WHERE_NO_ORMPRI {
		keys = keys[1:]
		if len(keys) == 0 {
			panic("no key set when set no ormpri")
		}
		ormGetType = typeoper.ORM_FOR_DEF_NOPRI
	}
	if ivalue.Kind() == reflect.Struct {
		vvalue := ivalue
		vType := vvalue.Type()
		stTags := sq.dao.collect.GetOrmStructTagInfo(&vType)
		keys, vals := sq.dao.collect.GetOrmKeyValInfo(stTags, vvalue, ormGetType, keys...)
		if keys == nil {
			panic("failed to get orm def info")
		}

		sq.sql = sq.sql + strings.Join(keys, "=? and ")
		sq.slConditions += strings.Join(keys, "=? and ")
		sq.sql = sq.sql + "=?)"
		sq.slConditions += "=?)"

		sq.condVal = append(sq.condVal, vals...)
		if sq.usage == SQL_FOR_SELECTUPDATE {
			sq.condNum += len(vals)
			sq.slVals = append(sq.slVals, vals...)
		}
	} else if ivalue.Kind() == reflect.Slice || ivalue.Kind() == reflect.Array {
		vvalue := ivalue.Index(0)
		vType := vvalue.Type()
		for vType.Kind() == reflect.Ptr {
			vvalue = vvalue.Elem()
			vType = vType.Elem()
		}
		if vType.Kind() != reflect.Struct {
			panic("not struct slice or array")
		}

		stTags := sq.dao.collect.GetOrmStructTagInfo(&vType)
		rkeys := sq.dao.collect.GetOrmKeyInfo(stTags, vvalue, ormGetType, keys...)
		if rkeys == nil || len(rkeys) == 0 {
			panic("failed to get orm def info")
		}

		sq.sql += " (" + strings.Join(rkeys, ",") + ") in ("
		sq.slConditions += " (" + strings.Join(rkeys, ",") + ") in ("
		ssimbols := "(" + strings.Repeat("?,", len(rkeys)-1)
		ssimbols += "?)"
		for i := 0; i < ivalue.Len(); i++ {
			vvalue = ivalue.Index(i)
			vType := vvalue.Type()
			for vType.Kind() == reflect.Ptr {
				vvalue = vvalue.Elem()
				vType = vType.Elem()
			}
			vals := sq.dao.collect.GetOrmValInfo(stTags, vvalue, ormGetType, keys...)
			defer clearVals(vals)
			if i < ivalue.Len()-1 {
				sq.sql += ssimbols + ","
				sq.slConditions += ssimbols + ","
			} else {
				sq.sql += ssimbols
				sq.slConditions += ssimbols
			}
			sq.condVal = append(sq.condVal, vals...)

			if sq.usage == SQL_FOR_SELECTUPDATE {
				sq.condNum += len(vals)
				sq.slVals = append(sq.slVals, vals...)
			}
		}
		sq.sql += "))"
		sq.slConditions += "))"
	}
	sq.status = SQL_IN_WHERE
	return sq
}

func (sq *SqlQuery) Delete(table string) *SqlQuery {
	sq.reset()
	sq.usage = SQL_FOR_DELETE
	sq.sql += "delete from " + table + " "
	return sq
}

func (sq *SqlQuery) RowsAffect() int {
	return sq.rowsAffect
}

func (sq *SqlQuery) Count() int {
	return sq.count
}

func (sq *SqlQuery) Fetch(i int) interface{} {
	if sq.usage != SQL_FOR_SELECT {
		return nil
	}
	if i >= len(sq.dstVals) {
		return nil
	}
	//return reflect.Indirect(*(sq.dstVals[i])).Addr().Interface()
	return sq.dstVals[i]
}

func (sq *SqlQuery) Release() {
	sq.reset()
	sq.dao.Lock()
	defer sq.dao.Unlock()
	sq.next = sq.dao.freeQuery
	sq.dao.freeQuery = sq
}

///输出sql语句用
var zeroDateTime = []byte("0000-00-00 00:00:00.000000")

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringBackslash is similar to escapeBytesBackslash but for string.
func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

func InterpolateParams(query string, args []interface{}) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf := make([]byte, 0, len(query))
	buf = buf[:0]
	argPos := 0

	rargs := make([]interface{}, len(args))

for_args_loop:
	for i := 0; i < len(args); i++ {
		if args[i] == nil {
			rargs[i] = nil
			continue
		}
		rv := reflect.ValueOf(args[i])
		for rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				rargs[i] = nil
				continue for_args_loop
			}
			rv = rv.Elem()
		}

		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			rargs[i] = rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rargs[i] = int64(rv.Uint())
		case reflect.Float32, reflect.Float64:
			rargs[i] = rv.Float()
		default:
			rargs[i] = rv.Interface()
		}
	}

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := rargs[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				v := v.In(time.Local)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				//buf = escapeBytesBackslash(buf, v)
				buf = escapeBytesQuotes(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			//buf = escapeStringBackslash(buf, v)
			buf = escapeStringQuotes(buf, v)
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}

		if len(buf)+4 > 4<<20 {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}
