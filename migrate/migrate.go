package migrate

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
)

func NewMigrator(db *sql.DB, path string) *migrate.Migrate {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatalln(err.Error())
	}

	e, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", path), "postgres", driver)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return e
}
