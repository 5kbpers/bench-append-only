package bench

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DB interface {
	CreateTables() error
	InsertBatch(base uint64, pace uint64, batchSize uint64) error
}

type mysqlDB struct {
	tables uint64
	dsn    string
	db     *sql.DB
}

func (m *mysqlDB) InsertBatch(base uint64, pace uint64, batchSize uint64) error {
	for i := uint64(0); i < m.tables; i++ {
		tableName := fmt.Sprintf("test%d", i)
		sql := fmt.Sprintf("INSERT INTO %s (id) VALUES (%d)", tableName, base)
		for j := uint64(1); j < batchSize; j++ {
			sql += fmt.Sprintf(",(%d)", base+j*pace)
		}
		if _, err := m.db.Exec(sql); err != nil {
			return err
		}
	}
	return nil
}

func (m *mysqlDB) CreateTables() error {
	for i := uint64(0); i < m.tables; i++ {
		tableName := fmt.Sprintf("test%d", i)
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id bigint PRIMARY KEY);", tableName)
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
