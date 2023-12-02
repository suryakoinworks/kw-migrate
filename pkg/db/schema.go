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

func (s schema) ListTable(nWorker int, name string, excludes ...string) <-chan []string {
	cTable := make(chan []string, nWorker)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_TABLE, name))
	if err != nil {
		fmt.Println(err.Error())

		return cTable
	}

	go func(result *sql.Rows, channel chan<- []string) {
		tables := [][]string{}
		n := 0
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
				i := (n % 10)
				if n < 10 {
					tables = append(tables, []string{})
				}

				tables[i] = append(tables[i], table)

				skip = false
			}

			n++
		}

		rows.Close()

		for _, t := range tables {
			channel <- t
		}

		close(channel)
	}(rows, cTable)

	return cTable
}
