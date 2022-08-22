package migrate

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
)

func NewMigrator(driver string, db *sql.DB, path string) *migrate.Migrate {
	var d database.Driver
	if driver == "posgres" {
		d = postgresql(db)
	}

	e, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", path), "postgres", d)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return e
}

func postgresql(db *sql.DB) database.Driver {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatalln(err.Error())
	}

	return driver
}
