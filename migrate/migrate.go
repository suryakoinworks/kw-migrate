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

func NewMigrator(db *sql.DB, database, schema string, path string) *migrate.Migrate {
	driver, err := postgres.WithInstance(db, &postgres.Config{SchemaName: schema})
	if err != nil {
		log.Fatalln(err.Error())
	}

	migrate, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", path), database, driver)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return migrate
}
