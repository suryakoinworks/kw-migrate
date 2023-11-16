package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type sync struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewSync(config config.Migration, errorColor *color.Color, successColor *color.Color) sync {
	return sync{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (s sync) Run(cluster string, schema string) error {
	lists, ok := s.config.Cluster[cluster]
	if !ok {
		s.errorColor.Printf("Cluster '%s' isn't defined\n", cluster)

		return nil
	}

	connections := map[string]config.Connection{}
	for _, c := range lists {
		if s.config.Source == c {
			continue
		}

		if _, ok := s.config.Connections[c]; !ok {
			s.errorColor.Printf("Connection '%s' isn't defined\n", c)

			return nil
		}

		connections[c] = s.config.Connections[c]
	}

	for i, source := range connections {
		db, err := config.NewConnection(source)
		if err != nil {
			s.errorColor.Println(err.Error())

			return nil
		}

		migrator := config.NewMigrator(db, source.Name, schema, fmt.Sprintf("%s/%s", s.config.Folder, schema))

		progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", i, schema)
		progress.Start()

		err = migrator.Up()
		if err != nil && err == gomigrate.ErrNoChange {
			progress.Stop()

			continue
		}

		version, dirty, _ := migrator.Version()
		if version != 0 && dirty {
			migrator.Force(int(version))
			migrator.Steps(-1)
		}

		progress.Stop()
	}

	s.successColor.Printf("Migration synced on %s schema %s\n", cluster, schema)

	return nil
}
