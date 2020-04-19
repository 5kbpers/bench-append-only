package bench

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DB interface {
	CreateTables() error
	Insert(uint64) error
}

type mysqlDB struct {
	tables uint64
	dsn    string
	db     *sql.DB
}

func (m *mysqlDB) Insert(seq uint64) error {
	for i := uint64(0); i < m.tables; i++ {
		tableName := fmt.Sprintf("test%d", i)
		sql := fmt.Sprintf("INSERT INTO %s (id) VALUES (%d);", tableName, seq)
		if _, err := m.db.Exec(sql); err != nil {
			return err
		}
	}
	return nil
}

func (m *mysqlDB) CreateTables() error {
	for i := uint64(0); i < m.tables; i++ {
		tableName := fmt.Sprintf("test%d", i)
		sql := fmt.Sprintf("CREATE TABLE %s IF NOT EXISTS (id bigint PRIMARY KEY);", tableName)
		if _, err := m.db.Exec(sql); err != nil {
			return err
		}
	}
	return nil
}

func newMySQLConn(tables uint64, dsn string) (DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &mysqlDB{
		tables: tables,
		dsn:    dsn,
		db:     db,
	}, nil
}
