package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/fatih/color"
)

type clean struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewClean(config config.Migration, errorColor *color.Color, successColor *color.Color) clean {
	return clean{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (c clean) Call(source string, schema string) error {
	dbConfig, ok := c.config.Connections[source]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", source)

		return nil
	}

	_, ok = c.config.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))

	version, dirty, _ := migrator.Version()
	if version != 0 && dirty {
		migrator.Force(int(version))
		migrator.Steps(-1)
	}

	c.successColor.Printf("Migration cleaned on %s schema %s\n", source, schema)

	return err
}
