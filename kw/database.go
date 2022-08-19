package kw

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
)

func Connect(database Database) (*sql.DB, error) {
	switch database.Driver {
	case "postgresql":
		return postgresql(database)
	}

	return nil, errors.New("unsupported driver")
}

func postgresql(database Database) (*sql.DB, error) {
	return sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", database.Host, database.Port, database.User, database.Password, database.Name))
}
