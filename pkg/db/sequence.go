package db

import (
	"database/sql"
	"fmt"
	"strings"
)

type sequence struct {
	db *sql.DB
}

func NewSequence(db *sql.DB) sequence {
	return sequence{db: db}
}

func (s sequence) GenerateDdl(schema string) <-chan Migration {
	cMigration := make(chan Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_SEQUENCE, schema))
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

			channel <- Migration{
				Name:       strings.Replace(name, ".", "_", -1),
				UpScript:   fmt.Sprintf(SQL_CREATE_SEQUENCE, name),
				DownScript: fmt.Sprintf(SECURE_DROP_SEQUENCE, name),
			}
		}

		close(channel)
		rows.Close()
	}(rows, cMigration)

	return cMigration
}
