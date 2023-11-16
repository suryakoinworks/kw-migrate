package db

import (
	"database/sql"
	"fmt"
	"strings"
)

type enum struct {
	db *sql.DB
}

func NewEnum(db *sql.DB) enum {
	return enum{db: db}
}

func (s enum) GenerateDdl(schema string) []migration {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_ENUM, schema))
	if err != nil {
		fmt.Println(err.Error())

		return []migration{}
	}
	defer rows.Close()

	udts := []migration{}
	for rows.Next() {
		var name string
		var values string
		err = rows.Scan(&name, &values)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		shortName := name
		sName := strings.Split(name, ".")
		if len(sName) == 2 {
			shortName = sName[1]
		}

		udts = append(udts, migration{
			Name:       shortName,
			UpScript:   s.createDdl(name, values),
			DownScript: fmt.Sprintf("DROP TYPE %s;", name),
		})
	}

	return udts
}

func (s enum) createDdl(name string, values string) string {
	ddl := fmt.Sprintf("CREATE TYPE %s AS ENUM (", name)

	for _, s := range strings.Split(values, "#") {
		ddl = fmt.Sprintf("%s '%s',", ddl, s)
	}

	ddl = strings.TrimRight(ddl, ",")
	ddl = fmt.Sprintf("%s);", ddl)

	return ddl
}
