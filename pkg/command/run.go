package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/fatih/color"
)

type run struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewRun(config config.Migration) run {
	return run{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (r run) Call(source string, schema string, step int) error {
	dbConfig, ok := r.config.Connections[source]
	if !ok {
		r.errorColor.Printf("Database connection '%s' not found\n", r.boldFont.Sprint(source))

		return nil
	}

	_, ok = r.config.Schemas[schema]
	if !ok {
		r.errorColor.Printf("Schema '%s' not found\n", r.boldFont.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		r.errorColor.Println(err.Error())

		return nil
	}

	if step <= 0 {
		r.errorColor.Println("Invalid step")

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", r.config.Folder, schema))
	err = migrator.Steps(step)
	if err != nil {
		r.errorColor.Println(err.Error())

		return nil
	}

	r.successColor.Printf("Migration on %s schema %s run successfully\n", r.boldFont.Sprint(source), r.boldFont.Sprint(schema))

	return nil
}
