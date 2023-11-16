package db

import (
	"database/sql"
	"fmt"
)

type materialized struct {
	db *sql.DB
}

func NewMaterializedView(db *sql.DB) materialized {
	return materialized{db: db}
}

func (s materialized) GenerateDdl(schema string) []migration {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_MATERIALIZED_VIEW, schema))
	if err != nil {
		fmt.Println(err.Error())

		return []migration{}
	}
	defer rows.Close()

	migrations := []migration{}
	for rows.Next() {
		var name string
		var definition string
		err = rows.Scan(&name, &definition)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		migrations = append(migrations, migration{
			Name:       name,
			UpScript:   definition,
			DownScript: fmt.Sprintf("DROP VIEW IF EXISTS %s;", name),
		})
	}

	return migrations
}
