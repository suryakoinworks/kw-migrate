package command

import (
	"fmt"
	"kmt/pkg/config"
	"os"
	"time"

	"github.com/fatih/color"
)

type create struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewCreate(config config.Migration, errorColor *color.Color, successColor *color.Color) create {
	return create{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (c create) Call(schema string, name string) error {
	_, ok := c.config.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	os.MkdirAll(fmt.Sprintf("%s/%s", c.config.Folder, schema), 0777)

	version := time.Now().Unix()
	name = fmt.Sprintf("%d_%s", version, name)
	_, err := os.Create(fmt.Sprintf("%s/%s/%s.up.sql", c.config.Folder, schema, name))
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	_, err = os.Create(fmt.Sprintf("%s/%s/%s.down.sql", c.config.Folder, schema, name))
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	c.successColor.Printf("Migration created as %s\n", c.successColor.Sprintf(name))

	return err
}
