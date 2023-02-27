package main

import (
	"fmt"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jiangdamalong/common/log"
	"github.com/jiangdamalong/common/stsql"
	//"github.com/jinzhu/gorm"
)

type Test struct {
	ID int   `gorm:"primary;column:id;auto_increment" json:"id"`
	V  Value `gorm:"struct;column:test" json:"test"`
}

type Value struct {
	ID    int    `gorm:"primary;column:id;auto_increment" json:"id"`
	VName string `gorm:"type:varchar(128);column:val_name" json:"val_name"`
	VAge  int    `gorm:"type:int;column:val_age" json:"val_age"`
}

type TestSum struct {
	Test1 Value `gorm:"struct;column:test" json:"test"`
	Test2 Value `gorm:"struct;column:test_1" json:"test_1"`
}

type Test3 struct {
	ID     int    `gorm:"column:id;primary" json:"id"`
	VName  string `gorm:"column:val_name" json:"val_name"`
	VAgent int    `gorm:"column:val_agent;primary" json:"val_agent"`
}

type TestFree struct {
	ID int `gorm:"column:id;primary" json:"id"`
}

func main() {
	/*var err error
	db, err := gorm.Open("mysql", "vod_web:hgKnhcwdxHPtny4Ad7bO@tcp(10.1.51.19:3306)/gorm_test?charset=utf8&parseTime=True&loc=Local")
	if err != nil || db == nil {
		panic(err)
	}
	db.SingularTable(true)
	var t Test
	t.VName = "test"
	t.VAge = 1
	db.Create(&t)
	fmt.Printf("%+v\n", t)*/
	/*db, _ := sql.Open("mysql", "vod_web:hgKnhcwdxHPtny4Ad7bO@tcp(10.1.51.19:3306)/gorm_test")
	stmt, _ := db.Prepare("select * from test")
	rows, _ := stmt.Query()
	fmt.Printf("rows %+v\n", rows)
	for rows.Next() {
		fmt.Printf("%+v\n", rows.Scan())
	}*/
	log.SetLevel(7)
	var dao stsql.SqlDao
	var query stsql.SqlQuery
	//dao.InitOpen("10.1.51.19:3306", "vod_web", "hgKnhcwdxHPtny4Ad7bO", "gorm_test")
	dao.InitOpen("10.1.104.13:6033", "cpp_vod_web", "ezx0qBwLUhLsPSh2CG1Z", "stt_yutang")
	query.Init(&dao, "$")

	/*var err error
	for i := 0; i < 10; i++ {
		go func() {
			var query stsql.SqlQuery
			query.Init(&dao, "$")
			_, err = dao.Conn().Exec("select sleep(1);")
			fmt.Printf("err %+v t %+v", err, t)
		}()
	}

	time.Sleep(40 * time.Second)*/
	var t Test3
	err := query.Select(reflect.TypeOf((*TestFree)(nil))).From("pet_test").Where(&t).Run()
	fmt.Printf("err %+v t %+v", err, t)
	query.Select(reflect.TypeOf((*Test)(nil)), "test.id as id", "val_name as test$val_name", "val_age as test$val_age").
		From("test").
		Where("val_age", "<=", 1).
		Or("val_name", "=", "test").
		Where("val_age", "<", stsql.DbStr("id-1")).
		Ext("order by id desc").Ext("limit 10").
		Run()
	query.Select(reflect.TypeOf((*Test)(nil))).
		From("test").Where("val_age", "<=", 1).
		Or("val_name", "=", "test").
		Where("val_age", "<", stsql.DbStr("id-1")).
		Ext("order by id asc").Ext("limit 10").
		Run()
	fmt.Printf("%+v\n\n", query)
	query.Select(reflect.TypeOf((*TestSum)(nil))).From("test", "test_1").Where("test.val_age", "=", stsql.DbStr("test_1.val_age")).Ext("group by test.id").Ext("order by test.id desc").Run()
	//fmt.Printf("%+v\n", query.Fetch(0).(*Test3))

	t.VName = "t"
	t.VAgent = 11
	t.ID = 123
	query.Insert("test_3", &t)
	t.VName = "aaa"
	fmt.Printf("%+v %+v\n", query, t)

	err = query.Update("test_3").Set(&t).Where(&t).Run()
	fmt.Printf("%+v %+v err %+v\n", query, t, err)

	var at [1]Test3
	t.ID = 123
	t.VAgent = 11
	t.VName = "mary111124"
	at[0] = t
	/*at[1] = t
	at[1].ID = 124*/
	time.Sleep(1 * time.Second)
	/*err = query.InsertDuplicate("test_3", &at, "val_name", "id", stsql.DbStr("val_agent=val_agent+1000003"))
	fmt.Printf("%+v %+v err %+v\n", query, at, err)

	err = query.Select(reflect.TypeOf((*Test3)(nil))).From("test_3").Where(&(at), stsql.SQL_WHERE_NO_ORMPRI, "val_name").Run()
	fmt.Printf("\n select %+v %+v err %+v\n\n", query, at, err)

	t.ID = 10
	err = query.Delete("test_3").Where(&at).Run()
	fmt.Printf("\n delete %+v %+v\n\n", err, query)

	err = query.SelectCount().From("test", "test_1").Where("test.id", "=", stsql.DbStr("test_1.id")).Run()
	fmt.Printf("%d %+v\n", query.Count(), query)

	t.VName = "kkkkkkkgaaa"
	err = query.SelectForUpdate("val_name").From("test_3").Where(&at).Update("test_3").Set(&t, "val_name").Run()
	fmt.Printf("\n%+v %+v\n", err, query)

	var vat [3]Value
	vat[0].VName = "xxxxx"
	vat[1].VName = "xxxxx1"
	vat[2].VName = "xxxxx2"
	err = query.Insert("test", &vat)
	fmt.Printf("\n%+v %+v\n", err, vat)*/
}
