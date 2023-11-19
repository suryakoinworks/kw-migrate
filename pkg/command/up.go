package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type up struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewUp(config config.Migration, errorColor *color.Color, successColor *color.Color) up {
	return up{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (u up) Call(source string, schema string) error {
	dbConfig, ok := u.config.Connections[source]
	if !ok {
		u.errorColor.Printf("Database connection '%s' not found\n", source)

		return nil
	}

	_, ok = u.config.Schemas[schema]
	if !ok {
		u.errorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		u.errorColor.Println(err.Error())

		return nil
	}

	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	if err != nil {
		u.errorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", u.config.Folder, schema))

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", source, schema)
	progress.Start()

	err = migrator.Up()
	if err != nil && err == gomigrate.ErrNoChange {
		progress.Stop()

		u.successColor.Printf("Database %s schema %s is up to date\n", source, schema)

		return nil
	}

	version, dirty, _ := migrator.Version()
	if version != 0 && dirty {
		migrator.Force(int(version))
		migrator.Steps(-1)
	}

	progress.Stop()

	u.successColor.Printf("Migration on %s schema %s run successfully\n", source, schema)

	return err
}
