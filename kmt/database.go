package kmt

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func Connect(database Connection) (*sql.DB, error) {
	return sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", database.Host, database.Port, database.User, database.Password, database.Name))
}
