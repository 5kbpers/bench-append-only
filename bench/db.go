package bench

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DB interface {
	RandomUpdate(id int64) error
}

type mysqlDB struct {
	dsn string
	db  *sql.DB
}

func (m *mysqlDB) RandomUpdate(id int64) error {
	sql := fmt.Sprintf("UPDATE cdctest SET time = now() where id=%d", id)
	if _, err := m.db.Exec(sql); err != nil {
		return err
	}
	return nil
}

func newMySQLConn(dsn string) (DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &mysqlDB{
		dsn: dsn,
		db:  db,
	}, nil
}
