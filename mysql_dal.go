package mysqldal

import (
	"database/sql"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MySqlConnection struct {
	ConnectionString string
	DB               *sql.DB
	Tx               *sql.Tx
}

func GetNewConn(connStr string) *MySqlConnection {
	conn := MySqlConnection{
		ConnectionString: connStr,
		DB:               nil,
		Tx:               nil,
	}

	return &conn
}

func (conn *MySqlConnection) Open() error {
	db, err := sql.Open("mysql", conn.ConnectionString)
	if err == nil {
		conn.DB = db
	}
	return err
}

func (conn *MySqlConnection) Close() error {
	return conn.DB.Close()
}

func (conn *MySqlConnection) Query(sqlStatements string, args ...interface{}) ([]map[string]string, error) {
	var rows *sql.Rows
	var err error
	if conn.Tx == nil {
		rows, err = conn.DB.Query(sqlStatements, args...)
	} else {
		rows, err = conn.Tx.Query(sqlStatements, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colLen := len(cols)
	vals := make([][]byte, colLen)
	scans := make([]interface{}, colLen)
	for i := range vals {
		scans[i] = &vals[i]
	}
	colTypes, err := rows.ColumnTypes()
	colTypesStrs := make([]string, len(colTypes))
	for i := range colTypes {
		colTypesStrs[i] = strings.ToUpper(colTypes[i].DatabaseTypeName())
	}

	var results []map[string]string
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]string)
		for k, v := range vals {
			key := cols[k]
			if v != nil {
				if colTypesStrs[k] == "BIT" {
					row[key] = strconv.Itoa(int(v[0]))
				} else {
					row[key] = string(v)
				}
			} else {
				row[key] = ""
			}
		}
		results = append(results, row)
	}

	return results, err
}

func (conn *MySqlConnection) QueryFirst(sqlStatements string, args ...interface{}) (map[string]string, error) {
	results, err := conn.Query(sqlStatements, args...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return make(map[string]string), err
	}

	return results[0], err
}

func (conn *MySqlConnection) ExecuteScalar(sqlStatements string, args ...interface{}) (string, error) {
	var err error
	var result string
	if conn.Tx == nil {
		err = conn.DB.QueryRow(sqlStatements, args...).Scan(&result)
	} else {
		err = conn.Tx.QueryRow(sqlStatements, args...).Scan(&result)
	}
	if err != nil {
		return "", err
	}

	return result, err
}

func (conn *MySqlConnection) ExecuteScalarInt(sqlStatements string, args ...interface{}) (*int64, error) {
	var err error
	var result *int64
	if conn.Tx == nil {
		err = conn.DB.QueryRow(sqlStatements, args...).Scan(result)
	} else {
		err = conn.Tx.QueryRow(sqlStatements, args...).Scan(result)
	}
	if err != nil {
		return nil, err
	}

	return result, err
}

func (conn *MySqlConnection) Execute(sqlStatements string, args ...interface{}) (int64, error) {
	var result int64
	var err error
	var dbResult sql.Result
	if conn.Tx == nil {
		dbResult, err = conn.DB.Exec(sqlStatements, args...)
	} else {
		dbResult, err = conn.Tx.Exec(sqlStatements, args...)
	}
	if err != nil {
		return 0, err
	}

	result, err = dbResult.RowsAffected()
	if err != nil {
		return 0, err
	}

	return result, err
}

func (conn *MySqlConnection) BeginTx() (*sql.Tx, error) {
	tx, err := conn.DB.Begin()
	if err == nil {
		conn.Tx = tx
	}

	return tx, err
}

func (conn *MySqlConnection) Commit() error {
	return conn.Tx.Commit()
}

func (conn *MySqlConnection) Rollback() error {
	return conn.Tx.Rollback()
}
