package db

import (
	"database/sql"
	"fmt"
)

type (
	schema struct {
		db *sql.DB
	}
)

func NewSchema(db *sql.DB) schema {
	return schema{db: db}
}

func (s schema) ListTables(name string, excludes ...string) []string {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_TABLE, name))
	if err != nil {
		fmt.Println(err.Error())

		return []string{}
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		skip := false
		for _, v := range excludes {
			if v == table {
				skip = true

				break
			}
		}

		if !skip {
			tables = append(tables, table)
			skip = false
		}
	}

	return tables
}
