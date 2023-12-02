package db

import (
	"database/sql"
	"fmt"
)

type primaryKey struct {
	db *sql.DB
}

func NewPrimaryKey(db *sql.DB) primaryKey {
	return primaryKey{db: db}
}

func (s primaryKey) GenerateDdl(schema string, table string) <-chan Migration {
	cMigration := make(chan Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_PRIMARY_KEY_IN_TABLE, schema, schema, table))
	if err != nil {
		fmt.Println(err.Error())

		return cMigration
	}

	go func(result *sql.Rows, channel chan<- Migration) {
		for result.Next() {
			var name string
			var definition string
			err = result.Scan(&name, &definition)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}

			channel <- Migration{
				Name:       name,
				UpScript:   fmt.Sprintf(SQL_CREATE_PRIMARY_KEY, schema, table, name, definition),
				DownScript: fmt.Sprintf(SECURE_DROP_PRIMARY_KEY, schema, table, name),
			}
		}

		close(channel)
		rows.Close()
	}(rows, cMigration)

	return cMigration
}
