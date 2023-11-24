package command

import (
	"fmt"
	"kmt/pkg/config"
	"os"
	"strconv"
	"strings"

	"github.com/briandowns/spinner"
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
	if step <= 0 {
		r.errorColor.Println("Invalid step")

		return nil
	}

	dbConfig, ok := r.config.Connections[source]
	if !ok {
		r.errorColor.Printf("Database connection '%s' not found\n", r.boldFont.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		r.errorColor.Printf("Schema '%s' not found\n", r.boldFont.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		r.errorColor.Println(err.Error())

		return nil
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", r.config.Folder, schema))
	if err != nil {
		r.errorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", r.config.Folder, schema))
	version, _, _ := migrator.Version()
	valid := false

	migrations := make([]string, step)
	number := 0
	for _, file := range files {
		f := strings.Split(file.Name(), "_")
		s, _ := strconv.Atoi(f[0])
		if version == uint(s) {
			valid = true

			break
		}

		if valid && number < step {
			migrations = append(migrations, f[0])

			number++
		}
	}

	for _, v := range migrations {
		progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.Suffix = fmt.Sprintf(" Run migration file %s on schema %s", r.boldFont.Sprint(v), r.boldFont.Sprint(schema))

		err = migrator.Steps(1)
		if err != nil {
			progress.Stop()
			r.errorColor.Printf("Error when running %s with message %s\n", r.boldFont.Sprint(v), r.boldFont.Sprint(err.Error()))

			return nil
		}

		progress.Stop()
	}

	r.successColor.Printf("Migration on %s schema %s run successfully\n", r.boldFont.Sprint(source), r.boldFont.Sprint(schema))

	return nil
}
