package db

import (
	"database/sql"
	"fmt"
)

type index struct {
	db *sql.DB
}

func NewIndex(db *sql.DB) index {
	return index{db: db}
}

func (s index) GenerateDdl(schema string, table string) <-chan Migration {
	cMigration := make(chan Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_INDEX_IN_TABLE, schema, table))
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
				UpScript:   definition,
				DownScript: fmt.Sprintf(SECURE_DROP_INDEX, schema, name),
			}
		}

		close(channel)
		rows.Close()
	}(rows, cMigration)

	return cMigration
}
