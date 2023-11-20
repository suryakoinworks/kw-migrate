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
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewSync(config config.Migration) sync {
	return sync{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (s sync) Run(cluster string, schema string) error {
	lists, ok := s.config.Cluster[cluster]
	if !ok {
		s.errorColor.Printf("Cluster '%s' isn't defined\n", s.boldFont.Sprint(cluster))

		return nil
	}

	connections := map[string]config.Connection{}
	for _, c := range lists {
		if s.config.Source == c {
			continue
		}

		if _, ok := s.config.Connections[c]; !ok {
			s.errorColor.Printf("Connection '%s' isn't defined\n", s.boldFont.Sprint(c))

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
		progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", s.boldFont.Sprint(i), s.boldFont.Sprint(schema))
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

	s.successColor.Printf("Migration synced on %s schema %s\n", s.boldFont.Sprint(cluster), s.boldFont.Sprint(schema))

	return nil
}
