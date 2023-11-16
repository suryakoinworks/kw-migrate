package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/fatih/color"
)

type version struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewVersion(config config.Migration, errorColor *color.Color, successColor *color.Color) version {
	return version{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (v version) Call(source string, schema string) uint {
	dbConfig, ok := v.config.Connections[source]
	if !ok {
		v.errorColor.Printf("Database connection '%s' not found\n", source)

		return 0
	}

	_, ok = v.config.Schemas[schema]
	if !ok {
		v.errorColor.Printf("Schema '%s' not found\n", schema)

		return 0
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", v.config.Folder, schema))
	version, _, err := migrator.Version()
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0
	}

	return version
}
