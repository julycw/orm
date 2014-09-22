package orm

import (
	"database/sql"
	"fmt"
	_ "github.com/weigj/go-odbc/driver"
	"log"
	"reflect"
	"time"
)

var (
	dbName   string
	dbUser   string
	dbPasswd string
	dbHost   string
)

type OrderBy int

const (
	SQL_SELECT_BY_PAGE_STR  = "SELECT TOP %v %v FROM %v WHERE (%v > (SELECT MAX(%v) FROM (SELECT TOP %v %v FROM %v %v %v) AS T) %v)%v"
	SQL_SELECT_ONE_PAGE_STR = "SELECT TOP %v %v FROM %v %v %v"
	SQL_SELECT_COUNT_STR    = "SELECT COUNT(%v) FROM %v %v"
)

const (
	ASC OrderBy = iota // 默认为0，即升序
	DESC
)

type Order struct {
	Name string
	By   OrderBy
}

type Condition struct {
	Name    string
	Value   string
	Compare string
}

type DBStore struct {
	tableName string
	modelRel  interface{}
	lastSql   string
	pkCol     string
	fields    []string
	cols      []string
	colTypies map[string]reflect.Kind
	selectStr string
}

func RegisterOrm(databaseHost, databaseUser, databasePasswd, databaseName string) {
	dbHost = databaseHost
	dbUser = databaseUser
	dbPasswd = databasePasswd
	dbName = databaseName
}

func getConnection() (conn *sql.DB, err error) {
	connStr := fmt.Sprintf("driver={SQL Server};SERVER=%v;UID=%v;PWD=%v;DATABASE=%v", dbHost, dbUser, dbPasswd, dbName)
	conn, err = sql.Open("odbc", connStr)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return conn, nil
}
func GetDBStore(tableName string, modelRel interface{}) (DBStore, error) {
	store := DBStore{}
	store.tableName = tableName
	store.modelRel = modelRel

	mType := reflect.TypeOf(modelRel)
	n := mType.Elem().NumField()

	store.fields = make([]string, n)
	store.cols = make([]string, n)
	store.colTypies = make(map[string]reflect.Kind)

	for i := 0; i < n; i++ {
		field := mType.Elem().Field(i)
		if store.pkCol == "" {
			if pk := field.Tag.Get("PK"); pk != "" {
				store.pkCol = field.Tag.Get("col")
				if store.pkCol == "" {
					store.pkCol = field.Name
				}
			}
		}

		if colName := field.Tag.Get("col"); colName == "" {
			store.cols[i] = field.Name
		} else {
			store.cols[i] = colName
		}

		store.fields[i] = field.Name
		store.colTypies[field.Name] = field.Type.Kind()
	}

	store.selectStr = store.MakeSelectWithCols(store.cols)
	return store, nil
}

func (this *DBStore) GetCountByCondition(conditions []Condition) (count int64) {
	whereStr := this.MakeWhereWithConditions(conditions)
	sql := fmt.Sprintf(SQL_SELECT_COUNT_STR, this.pkCol, this.tableName, whereStr)
	this.lastSql = sql
	if conn, err := getConnection(); err != nil {
		log.Println(err.Error())
	} else {
		defer conn.Close()
		if stmt, err := conn.Prepare(sql); err != nil {
			log.Println("Query Error", err)
		} else {
			defer stmt.Close()
			if row, err := stmt.Query(); err != nil {
				log.Println("Query Error", err)
			} else {
				if err := row.Scan(&count); err != nil {
					log.Println(err.Error())
				}
			}
		}
	}
	return count
}

func (this *DBStore) GetByPageAndConditionAndOrder(page, size int, conditions []Condition, orders []Order) []interface{} {
	fmt.Println(page, size, len(conditions), len(orders))

	orderStr := this.MakeOrderByWithOrders(orders)
	whereStr := this.MakeWhereWithConditions(conditions)
	var sql string
	if page == 1 {
		sql = fmt.Sprintf(SQL_SELECT_ONE_PAGE_STR, size, this.selectStr, this.tableName, whereStr, orderStr)
	} else {
		var andWhereStr string
		if whereStr != "" {
			andWhereStr = "AND " + whereStr[6:]
		}
		sql = fmt.Sprintf(SQL_SELECT_BY_PAGE_STR, size, this.selectStr, this.tableName, this.pkCol, this.pkCol, size*(page-1), this.pkCol, this.tableName, whereStr, orderStr, andWhereStr, orderStr)
	}
	this.lastSql = sql
	if conn, err := getConnection(); err != nil {
		log.Println(err.Error())
		return nil
	} else {
		defer conn.Close()
		if stmt, err := conn.Prepare(sql); err != nil {
			log.Println("Query Error", err)
			return nil
		} else {
			defer stmt.Close()
			if row, err := stmt.Query(); err != nil {
				log.Println("Query Error", err)
				return nil
			} else {
				defer row.Close()
				var resultList []interface{} = make([]interface{}, 0)
				for row.Next() {
					var resultRow []interface{} = make([]interface{}, len(this.cols), len(this.cols))
					for i, _ := range resultRow {
						switch this.colTypies[this.fields[i]] {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							resultRow[i] = new(int)
						case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
							resultRow[i] = new(uint)
						case reflect.Float32, reflect.Float64:
							resultRow[i] = new(float64)
						case reflect.String:
							resultRow[i] = new(string)
						case reflect.Bool:
							resultRow[i] = new(bool)
						case reflect.Struct:
							resultRow[i] = new(time.Time)
						default:
							//
						}
					}
					if err := row.Scan(resultRow...); err != nil {
						log.Println(err.Error())
					} else {
						for index, fieldName := range this.fields {
							switch this.colTypies[fieldName] {
							case reflect.String:
								reflect.ValueOf(this.modelRel).Elem().FieldByName(fieldName).SetString(*(resultRow[index].(*string)))
							case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
								value := *(resultRow[index].(*int))
								reflect.ValueOf(this.modelRel).Elem().FieldByName(fieldName).SetInt(int64(value))
							case reflect.Struct:
								value := *(resultRow[index].(*time.Time))
								// println(value.Format("2006-01-02 15:04:05"))
								reflect.ValueOf(this.modelRel).Elem().FieldByName(fieldName).Set(reflect.ValueOf(value))

							case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
							case reflect.Float32, reflect.Float64:
							case reflect.Bool:
							default:
								continue
							}

						}

						resultList = append(resultList, reflect.ValueOf(this.modelRel).Elem().Interface())
					}
				}
				return resultList
			}
		}
	}
}

func (this *DBStore) GetTableName() string {
	return this.tableName
}

func (this *DBStore) GetLastSQL() string {
	return this.lastSql
}

func (this OrderBy) String() string {
	switch this {
	case ASC:
		return "ASC"
	case DESC:
		return "DESC"
	default:
		return "ASC"
	}
}

func (this *DBStore) MakeSelectWithCols(cols []string) (selectString string) {
	for _, col := range cols {
		if col == "" {
			continue
		}

		if selectString != "" {
			selectString += ","
		}
		selectString += col
	}
	return selectString
}

func (this *DBStore) MakeWhereWithConditions(conditions []Condition) (whereString string) {
	for _, condition := range conditions {
		if condition.Name == "" || condition.Value == "" || condition.Compare == "" {
			continue
		}

		if whereString != "" {
			whereString += " AND "
		} else {
			whereString += "WHERE "
		}
		whereString += condition.Name + " " + condition.Compare + " " + condition.Value
	}

	return whereString
}

func (this *DBStore) MakeOrderByWithOrders(orders []Order) (orderString string) {
	for _, order := range orders {
		if order.Name == "" {
			continue
		}

		if orderString != "" {
			orderString += ","
		}
		orderString += fmt.Sprintf("%v %v", order.Name, order.By)
	}

	if orderString == "" {
		orderString += this.pkCol
	}

	return "ORDER BY " + orderString
}
