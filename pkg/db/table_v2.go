package db

import (
	"database/sql"
	"fmt"
)

type tableV2 struct {
	db *sql.DB
}

func NewTableV2(db *sql.DB) tableV2 {
	return tableV2{db: db}
}

func (s tableV2) GenerateDdl(schema string, table string) <-chan Migration {
	cMigration := make(chan Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_COLUMN_IN_TABLE, schema, table))
	if err != nil {
		fmt.Println(err.Error())

		return cMigration
	}

	go func(result *sql.Rows, channel chan<- Migration) {
		for result.Next() {
			var name string
			err = result.Scan(&name)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}
		}

		channel <- Migration{
			Name:       fmt.Sprintf("%s_%s", schema, table),
			UpScript:   "",
			DownScript: "",
		}

		close(channel)
		rows.Close()
	}(rows, cMigration)

	return cMigration
}
