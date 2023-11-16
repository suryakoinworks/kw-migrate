package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/fatih/color"
)

type compare struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewCompare(config config.Migration, errorColor *color.Color, successColor *color.Color) compare {
	return compare{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (v compare) Call(source string, compare string, schema string) (uint, uint) {
	dbSource, ok := v.config.Connections[source]
	if !ok {
		v.errorColor.Printf("Database connection '%s' not found\n", source)

		return 0, 0
	}

	dbCompare, ok := v.config.Connections[compare]
	if !ok {
		v.errorColor.Printf("Database connection '%s' not found\n", compare)

		return 0, 0
	}

	_, ok = v.config.Schemas[schema]
	if !ok {
		v.errorColor.Printf("Schema '%s' not found\n", schema)

		return 0, 0
	}

	connSource, err := config.NewConnection(dbSource)
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	connCompare, err := config.NewConnection(dbCompare)
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	sourceMigrator := config.NewMigrator(connSource, dbSource.Name, schema, fmt.Sprintf("%s/%s", v.config.Folder, schema))
	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	compareMigrator := config.NewMigrator(connCompare, dbCompare.Name, schema, fmt.Sprintf("%s/%s", v.config.Folder, schema))
	compareVersion, _, err := compareMigrator.Version()
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	return sourceVersion, compareVersion
}
