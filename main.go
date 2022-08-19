package main

import (
	"fmt"
	"koin-migrate/kw"
	"koin-migrate/migrate"
	"log"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

var (
	spinerIndex = 9
	duration    = 77 * time.Millisecond
)

func main() {
	app := &cli.App{
		Name:                 "kw-migrate",
		Usage:                "Koinworks Migration Tool",
		Description:          "kw-migrate up",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			{
				Name:        "up",
				Aliases:     []string{"u"},
				Description: "up",
				Usage:       "Migration Up",
				Action: func(*cli.Context) error {
					return nil
				},
			},
			{
				Name:        "down",
				Aliases:     []string{"d"},
				Description: "down",
				Usage:       "Migration Down",
				Action: func(ctx *cli.Context) error {
					return nil
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"c"},
				Description: "create",
				Usage:       "Create New Migration",
				Action: func(ctx *cli.Context) error {
					return nil
				},
			},
			{
				Name:        "from-db",
				Aliases:     []string{"f"},
				Description: "from-db",
				Usage:       "Create Migration from Existing Database",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					db, err := kw.Connect(config.Migrate.Database)
					if err != nil {
						return err
					}

					version := time.Now().Unix()

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Listing tables from schemas... "
					progress.Start()
					for k, v := range config.Migrate.Schemas {
						config.Migrate.Schemas[k]["tables"] = migrate.NewSchema(db, k).ListTables(v["excludes"])
					}

					progress.Stop()
					progress = spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Generating migration files... "
					progress.Start()

					ddl := migrate.NewDdl(config.Migrate.PgDump, config.Migrate.Database)
					for k, v := range config.Migrate.Schemas {
						schema := color.New(color.FgGreen).Sprint(k)

						for _, t := range v["tables"] {
							progress.Stop()
							progress = spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Processing schema %s table %s... ", schema, color.New(color.FgGreen).Sprint(t))
							progress.Start()

							schemaOnly := true
							for _, d := range v["with_data"] {
								if d == t {
									schemaOnly = false

									break
								}
							}

							upscript, downscript := ddl.Generate(fmt.Sprintf("%s.%s", k, t), schemaOnly)

							err := os.WriteFile(fmt.Sprintf("%s/%d_%s.up.sql", config.Migrate.Folder, version, t), []byte(upscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%d_%s.down.sql", config.Migrate.Folder, version, t), []byte(downscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}
						}
					}

					progress.Stop()

					return nil
				},
			},
			{
				Name:        "init",
				Aliases:     []string{"i"},
				Description: "init",
				Usage:       "Create Configuration File",
				Action: func(ctx *cli.Context) error {
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
