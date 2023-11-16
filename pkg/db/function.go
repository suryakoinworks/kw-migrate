package db

import (
	"database/sql"
	"fmt"
)

type function struct {
	db *sql.DB
}

func NewFunction(db *sql.DB) function {
	return function{db: db}
}

func (s function) GenerateDdl(schema string) []migration {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_FUNCTION, schema))
	if err != nil {
		fmt.Println(err.Error())

		return []migration{}
	}
	defer rows.Close()

	migrations := []migration{}
	for rows.Next() {
		var name string
		var definition string
		var params string
		err = rows.Scan(&name, &definition, &params)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		migrations = append(migrations, migration{
			Name:       name,
			UpScript:   definition,
			DownScript: fmt.Sprintf("DROP FUNCTION IF EXISTS %s(%s);", name, params),
		})
	}

	return migrations
}