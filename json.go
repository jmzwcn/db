package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	DB             *sql.DB
	alreadyCreated = false
)

func InitDB(db *sql.DB, tables ...string) {
	DB = db
	for _, v := range tables {
		checkTable(v)
	}
	alreadyCreated = len(tables) > 0
}

func checkTable(table string) error {
	if alreadyCreated {
		return nil
	}
	sql := "CREATE TABLE IF NOT EXISTS ? (data JSON, id VARCHAR(64) GENERATED ALWAYS AS (data->'$.id') VIRTUAL, INDEX idx (id))"
	_, err := DB.Exec(sql, table)
	return err
}

func Insert(table string, obj proto.Message) error {
	tx, err := DB.Begin()
	if err != nil {
		return err
	}
	if err := InsertTx(tx, table, obj); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func InsertTx(tx *sql.Tx, table string, obj proto.Message) error {
	checkTable(table)
	jsonv, err := protojson.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT INTO "+table+"(data) VALUES (CAST(? AS JSON))", jsonv)
	return err
}

func InsertIfNotExist(table string, id interface{}, obj proto.Message) error {
	if GetById(table, id, proto.Clone(obj)) == sql.ErrNoRows {
		return Insert(table, obj)
	}
	return nil
}

// Update||Insert
func Upsert(table string, id interface{}, obj proto.Message) error {
	if err := GetById(table, id, proto.Clone(obj)); err == sql.ErrNoRows {
		return Insert(table, obj)
	}
	return Update(table, id, obj)
}

func GetById(table string, id interface{}, obj proto.Message) error {
	return Get(table, map[string]interface{}{"$.id": id}, obj)
}

// https://dev.mysql.com/doc/refman/5.7/en/json.html#json-paths
func Get(table string, kvs map[string]interface{}, obj proto.Message) error {
	checkTable(table)
	var (
		keys   []string
		values []interface{}
	)
	for k, v := range kvs { // key should be [json-path], e.g:$.id
		keys = append(keys, "data->'"+k+"'=?")
		values = append(values, v)
	}
	query := "SELECT data FROM " + table + " WHERE " + strings.Join(keys, " AND ")
	data := ""
	if err := DB.QueryRow(query, values...).Scan(&data); err == nil {
		if err := protojson.Unmarshal([]byte(data), obj); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}

func List(table string, result interface{}, clause ...string) error {
	checkTable(table)
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()
	query := "SELECT data FROM " + table

	if len(clause) > 0 {
		query = query + " " + strings.Join(clause, " ")
	}
	log.Infoln(query)
	rows, err := DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		data := ""
		err := rows.Scan(&data)
		if err != nil {
			return err
		}
		elemp := reflect.New(elemt.Elem())
		protojson.Unmarshal([]byte(data), elemp.Interface().(proto.Message))
		slicev = reflect.Append(slicev, elemp)
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))
	return nil
}

func Update(table string, id interface{}, newObj proto.Message) error {
	tx, err := DB.Begin()
	if err != nil {
		return err
	}

	if err := UpdateTx(tx, table, id, newObj); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func UpdateTx(tx *sql.Tx, table string, id interface{}, newObj proto.Message) error {
	jsonv, err := protojson.Marshal(newObj)
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE "+table+" SET data=CAST(? AS JSON) WHERE data->'$.id'=?", jsonv, id)
	return err
}

func UpdateKVS(table string, id interface{}, kvs map[string]interface{}) error {
	var (
		keys   []string
		values []interface{}
	)
	for k, v := range kvs { // key should be [json-path], e.g:$.id
		keys = append(keys, ",'"+k+"',?")
		values = append(values, v)
	}
	sql := "UPDATE " + table + " SET data=" + "JSON_SET(data" + strings.Join(keys, "") + ") WHERE data->'$.id'= ?"
	_, err := DB.Exec(sql, append(values, id)...)
	return err
}

func Delete(table string, id interface{}) error {
	tx, err := DB.Begin()
	if err != nil {
		return err
	}
	if err := DeleteTx(tx, table, id); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func DeleteTx(tx *sql.Tx, table string, id interface{}) error {
	_, err := tx.Exec("DELETE FROM "+table+" WHERE data->'$.id' = ?", id)
	return err
}
