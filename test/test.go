//SQLSERVER 2005 下测试通过
package main

import (
	"fmt"
	"github.com/julycw/orm"
)

type Catalog struct {
	ID          int    `PK:"true" col:"id"` //PK表示该字段为主键
	Title       string `col:"title"`        //col表示该字段对应数据表中的列名
	Description string //如果不标识col，默认对应列名为字段名
}

func main() {
	dbName := "dbname"
	dbUser := "dbuser"
	dbPasswd := "dbPasswd"
	dbHost := "dbHost"

	orm.RegisterOrm(dbHost, dbUser, dbPasswd, dbName)

	//获取数据仓库，第一个参数表示数据库中的表名，第二个参数是数据模型的指针
	CatalogStore, _ := orm.GetDBStore("Cell_Catalog", &Catalog{})
	//获取数据列表
	catalogList := CatalogStore.GetByPageAndConditionAndOrder(
		//页码
		1,
		//页面大小
		20,
		//筛选条件
		[]orm.Condition{
			orm.Condition{Name: "title", Compare: "=", Value: "test1"},
			orm.Condition{Name: "descrption", Compare: "like", Value: "hello%"},
		},
		//排序规则
		[]orm.Order{
			orm.Order{Name: "title", By: orm.DESC},
		})

	for _, v := range catalogList {
		fmt.Println(v.(Catalog).Title)
	}

	//获取真实执行SQL语句
	fmt.Printf("sql:%v\n", CatalogStore.GetLastSQL())

	//获取数量
	count := CatalogStore.GetCountByCondition([]orm.Condition{
		orm.Condition{Name: "title", Compare: "=", Value: "test1"},
		orm.Condition{Name: "descrption", Compare: "like", Value: "hello%"},
	})

	fmt.Printf("count:%v\n", count)
	fmt.Printf("sql:%v\n", CatalogStore.GetLastSQL())

}
