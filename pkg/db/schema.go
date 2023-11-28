package db

import (
	"database/sql"
	"fmt"
	"strconv"
)

type (
	schema struct {
		db *sql.DB
	}
)

func NewSchema(db *sql.DB) schema {
	return schema{db: db}
}

func (s schema) CountTable(name string, nExcludes int) int {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_COUNT_TABLE, name))
	if err != nil {
		fmt.Println(err.Error())

		return 0
	}

	defer rows.Close()

	var total string
	for rows.Next() {
		err = rows.Scan(&total)
		if err != nil {
			fmt.Println(err.Error())

			return 0
		}
	}

	i, err := strconv.Atoi(total)
	if err != nil {
		fmt.Println(err.Error())

		return 0
	}

	return i - nExcludes
}

func (s schema) ListTable(name string, excludes ...string) <-chan string {
	cTable := make(chan string)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_TABLE, name))
	if err != nil {
		fmt.Println(err.Error())

		return cTable
	}

	go func(result *sql.Rows, channel chan<- string) {
		for result.Next() {
			var table string
			err = result.Scan(&table)
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
				channel <- table
				skip = false
			}
		}

		close(channel)
		rows.Close()
	}(rows, cTable)

	return cTable
}
