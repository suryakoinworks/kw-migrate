package main

import (
	"errors"
	"fmt"
	"koin-migrate/kw"
	"koin-migrate/migrate"
	"log"
	"os"
	"strconv"
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
		Name:                   "kw-migrate",
		Usage:                  "Koinworks Migration Tool",
		Description:            "kw-migrate help",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Commands: []*cli.Command{
			{
				Name:        "up",
				Aliases:     []string{"u"},
				Description: "up [<db>] [<schema>] [--all-connection] [--all-schema]",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "all-connection", Aliases: []string{"ac"}},
					&cli.BoolFlag{Name: "all-schema", Aliases: []string{"as"}},
				},
				Usage: "Migration Up",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					if ctx.Bool("all-connection") {
						for i, source := range config.Migrate.Connections {
							if config.Migrate.Source == i {
								continue
							}

							db, err := kw.Connect(source)
							if err != nil {
								return err
							}

							for k := range config.Migrate.Schemas {
								_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", k))
								if err != nil {
									return err
								}

								progress := spinner.New(spinner.CharSets[spinerIndex], duration)
								progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", i, k)
								progress.Start()

								migrator := migrate.NewMigrator(db, i, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
								err := migrator.Up()
								if err != nil {
									version, dirty, _ := migrator.Version()
									if version != 0 {
										if dirty {
											migrator.Force(int(version))
										}

										migrator.Steps(-1)
									}

									progress.Stop()

									return err
								}

								progress.Stop()
							}
						}

						return nil
					}

					if ctx.Bool("all-schema") {
						if ctx.NArg() != 1 {
							return errors.New("Not enough arguments. Usage: kw-migrate up <db> --all-schema")
						}

						source := ctx.Args().Get(0)
						dbConfig, ok := config.Migrate.Connections[source]
						if !ok {
							return errors.New(fmt.Sprintf("Config for '%s' not found", source))
						}

						db, err := kw.Connect(dbConfig)
						if err != nil {
							return err
						}

						for k := range config.Migrate.Schemas {
							_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", k))
							if err != nil {
								return err
							}

							progress := spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", source, k)
							progress.Start()

							migrator := migrate.NewMigrator(db, source, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
							err := migrator.Up()
							if err != nil {
								version, dirty, _ := migrator.Version()
								if version != 0 {
									if dirty {
										migrator.Force(int(version))
									}

									migrator.Steps(-1)
								}

								progress.Stop()

								return err
							}

							progress.Stop()
						}

						return nil
					}

					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate up <db> <schema>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						source := ctx.Args().Get(0)
						return errors.New(fmt.Sprintf("Config for '%s' not found", source))
					}

					db, err := kw.Connect(dbConfig)
					if err != nil {
						return err
					}

					if ctx.Args().Get(1) == "init" {
						progress := spinner.New(spinner.CharSets[spinerIndex], duration)
						progress.Suffix = "Run init scripts"
						progress.Start()

						for k := range config.Migrate.Schemas {
							_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", k))
							if err != nil {
								progress.Stop()

								return err
							}
						}

						dir := fmt.Sprintf("%s/init", config.Migrate.Folder)
						files, err := os.ReadDir(dir)
						if err != nil {
							progress.Stop()

							return err
						}

						for _, file := range files {
							sql, err := os.ReadFile(fmt.Sprintf("%s/%s", dir, file.Name()))
							if err != nil {
								progress.Stop()

								return err
							}

							_, err = db.Exec(string(sql))
							if err != nil {
								progress.Stop()

								return err
							}
						}

						progress.Stop()

						return nil
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(db, source, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", source, schema)
					progress.Start()

					err = migrator.Up()
					if err != nil {
						version, dirty, _ := migrator.Version()
						if version != 0 {
							if dirty {
								migrator.Force(int(version))
							}

							migrator.Steps(-1)
						}
					}

					progress.Stop()

					return err
				},
			},
			{
				Name:        "rollback",
				Aliases:     []string{"r"},
				Description: "rollback <db> <schema> <step>",
				Usage:       "Migration Rollback",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					if ctx.NArg() != 3 {
						return errors.New("Not enough arguments. Usage: kw-migrate rollback <db> <schema> <step>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						return errors.New(fmt.Sprintf("Config for '%s' not found", source))
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					db, err := kw.Connect(dbConfig)
					if err != nil {
						return err
					}

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil || n >= 0 {
						return errors.New("Invalid step")
					}

					migrator := migrate.NewMigrator(db, source, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Steps(int(n))
				},
			},
			{
				Name:    "down",
				Aliases: []string{"d"},
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "all-connection", Aliases: []string{"ac"}},
					&cli.BoolFlag{Name: "all-schema", Aliases: []string{"as"}},
				},
				Description: "down [<db>] [<schema>] [--all-connection] [--all-schema]",
				Usage:       "Migration Down",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					if ctx.Bool("all-connection") {
						for i, source := range config.Migrate.Connections {
							if config.Migrate.Source == i {
								continue
							}

							db, err := kw.Connect(source)
							if err != nil {
								return err
							}

							for k := range config.Migrate.Schemas {
								progress := spinner.New(spinner.CharSets[spinerIndex], duration)
								progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", i, k)
								progress.Start()

								migrator := migrate.NewMigrator(db, i, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
								err := migrator.Down()
								if err != nil {
									progress.Stop()

									return err
								}

								progress.Stop()
							}
						}

						return nil
					}

					if ctx.Bool("all-schema") {
						if ctx.NArg() != 1 {
							return errors.New("Not enough arguments. Usage: kw-migrate up <db> --all-schema")
						}

						source := ctx.Args().Get(0)
						dbConfig, ok := config.Migrate.Connections[source]
						if !ok {
							return errors.New(fmt.Sprintf("Config for '%s' not found", source))
						}

						db, err := kw.Connect(dbConfig)
						if err != nil {
							return err
						}

						for k := range config.Migrate.Schemas {
							progress := spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", source, k)
							progress.Start()

							migrator := migrate.NewMigrator(db, source, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
							err := migrator.Down()
							if err != nil {
								progress.Stop()

								return err
							}

							progress.Stop()
						}

						return nil
					}

					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate down <db> <schema>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						return errors.New(fmt.Sprintf("Config for '%s' not found", source))
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					db, err := kw.Connect(dbConfig)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(db, source, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", source, schema)
					progress.Start()

					err = migrator.Down()

					progress.Stop()

					return err
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"c"},
				Description: "create <schema> <name>",
				Usage:       "Create New Migration  for Schema",
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
					_, err := os.Create(fmt.Sprintf("%s/%s/%d_%s.up.sql", config.Migrate.Folder, schema, version, name))
					if err != nil {
						return err
					}

					_, err = os.Create(fmt.Sprintf("%s/%s/%d_%s.down.sql", config.Migrate.Folder, schema, version, name))

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
					source, ok := config.Migrate.Connections[config.Migrate.Source]
					if !ok {
						return errors.New(fmt.Sprintf("config for '%s' not found", config.Migrate.Source))
					}

					db, err := kw.Connect(source)
					if err != nil {
						return err
					}

					if ctx.NArg() == 1 {
						schema := ctx.Args().Get(0)
						progress := spinner.New(spinner.CharSets[spinerIndex], duration)
						progress.Suffix = " Listing tables from schemas... "
						progress.Start()

						_, ok := config.Migrate.Schemas[schema]
						if !ok {
							return errors.New("Schema not found")
						}

						config.Migrate.Schemas[schema]["tables"] = migrate.NewSchema(db, schema).ListTables(config.Migrate.Schemas[schema]["excludes"])

						ddl := migrate.NewDdl(config.Migrate.PgDump, source)
						version := time.Now().Unix()
						referenceScripts := map[string][]string{}
						foreignScripts := map[string][]string{}

						os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, schema), 0777)
						referenceScripts[schema] = []string{}
						foreignScripts[schema] = []string{}

						tlen := len(config.Migrate.Schemas[schema]["tables"])
						for j, t := range config.Migrate.Schemas[schema]["tables"] {
							progress.Stop()
							progress = spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Processing table %s (%d/%d)... ", color.New(color.FgGreen).Sprint(t), (j + 1), tlen)
							progress.Start()

							schemaOnly := true
							for _, d := range config.Migrate.Schemas[schema]["with_data"] {
								if d == t {
									schemaOnly = false

									break
								}
							}

							upscript, downscript, foreignScript, referenceScript := ddl.Generate(fmt.Sprintf("%s.%s", schema, t), schemaOnly)
							referenceScripts[schema] = append(referenceScripts[schema], referenceScript)
							foreignScripts[schema] = append(foreignScripts[schema], foreignScript)

							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, schema, version, t), []byte(upscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, schema, version, t), []byte(downscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
						}

						progress.Stop()
						progress = spinner.New(spinner.CharSets[spinerIndex], duration)
						progress.Suffix = " Mapping references..."
						progress.Start()

						for k, s := range referenceScripts {
							for i, c := range s {
								err := os.WriteFile(fmt.Sprintf("%s/%s/%d_reference_%d.up.sql", config.Migrate.Folder, k, version, i), []byte(c), 0777)
								if err != nil {
									progress.Stop()

									return err
								}

								version++
							}

						}

						for k, s := range foreignScripts {
							for i, c := range s {
								err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foregin_keys_%d.up.sql", config.Migrate.Folder, k, version, i), []byte(c), 0777)
								if err != nil {
									progress.Stop()

									return err
								}

								version++
							}

						}

						progress.Stop()

						return nil
					}

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
					slen := len(config.Migrate.Schemas)
					i := 1
					version := time.Now().Unix()
					referenceScripts := map[string][]string{}
					foreignScripts := map[string][]string{}
					for k, v := range config.Migrate.Schemas {
						os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, k), 0777)
						referenceScripts[k] = []string{}
						foreignScripts[k] = []string{}

						schema := color.New(color.FgGreen).Sprint(k)
						tlen := len(v["tables"])
						for j, t := range v["tables"] {
							progress.Stop()
							progress = spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Processing schema %s (%d/%d) table %s (%d/%d)... ", schema, i, slen, color.New(color.FgGreen).Sprint(t), (j + 1), tlen)
							progress.Start()

							schemaOnly := true
							for _, d := range v["with_data"] {
								if d == t {
									schemaOnly = false

									break
								}
							}

							upscript, downscript, foreignScript, referenceScript := ddl.Generate(fmt.Sprintf("%s.%s", k, t), schemaOnly)
							referenceScripts[k] = append(referenceScripts[k], referenceScript)
							foreignScripts[k] = append(foreignScripts[k], foreignScript)

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

							version++
						}

						i++
					}

					progress.Stop()
					progress = spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Mapping references..."
					progress.Start()

					for k, s := range referenceScripts {
						for i, c := range s {
							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_reference_%d.up.sql", config.Migrate.Folder, k, version, i), []byte(c), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
						}

					}

					for k, s := range foreignScripts {
						for i, c := range s {
							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foregin_keys_%d.up.sql", config.Migrate.Folder, k, version, i), []byte(c), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
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
