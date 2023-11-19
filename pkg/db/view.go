package db

import (
	"database/sql"
	"fmt"
)

type view struct {
	db *sql.DB
}

func NewView(db *sql.DB) view {
	return view{db: db}
}

func (s view) GenerateDdl(schema string) []Migration {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_VIEW, schema))
	if err != nil {
		fmt.Println(err.Error())

		return []Migration{}
	}
	defer rows.Close()

	migrations := []Migration{}
	for rows.Next() {
		var name string
		var definition string
		err = rows.Scan(&name, &definition)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		migrations = append(migrations, Migration{
			Name:       name,
			UpScript:   definition,
			DownScript: fmt.Sprintf("DROP VIEW IF EXISTS %s;", name),
		})
	}

	return migrations
}
