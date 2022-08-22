package main

import (
	"errors"
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
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate up <db> <schema>")
					}

					config := kw.Parse("Kwfile.yml")
					source, ok := config.Migrate.Database[ctx.Args().Get(0)]
					if !ok {
						return errors.New(fmt.Sprintf("Config for '%s' not found", ctx.Args().Get(0)))
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					db, err := kw.Connect(source)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(source.Driver, db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Up()
				},
			},
			{
				Name:        "down",
				Aliases:     []string{"d"},
				Description: "down",
				Usage:       "Migration Down",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate down <db> <schema>")
					}

					config := kw.Parse("Kwfile.yml")
					source, ok := config.Migrate.Database[ctx.Args().Get(0)]
					if !ok {
						return errors.New(fmt.Sprintf("Config for '%s' not found", ctx.Args().Get(0)))
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					db, err := kw.Connect(source)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(source.Driver, db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Down()
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"c"},
				Description: "create",
				Usage:       "Create New Migration",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate create <schema> <name>")
					}

					config := kw.Parse("Kwfile.yml")

					schema := ctx.Args().Get(0)
					_, ok := config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, schema), 0777)

					version := time.Now().Unix()

					name := ctx.Args().Get(1)
					_, err := os.Create(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, schema, version, name))
					if err != nil {
						return err
					}

					_, err = os.Create(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, schema, version, name))

					return err
				},
			},
			{
				Name:        "generate",
				Aliases:     []string{"gen"},
				Description: "generate",
				Usage:       "Generate Migration from Existing Database",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					source, ok := config.Migrate.Database[config.Migrate.Source]
					if !ok {
						return errors.New(fmt.Sprintf("config for '%s' not found", config.Migrate.Source))
					}

					db, err := kw.Connect(source)
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

					ddl := migrate.NewDdl(config.Migrate.PgDump, source)
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

							os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, k), 0777)

							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, k, version, t), []byte(upscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, k, version, t), []byte(downscript), 0777)
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
