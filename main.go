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
	gomigrate "github.com/golang-migrate/migrate/v4"
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
				Name:        "sync",
				Description: "sync <cluster> <schema>",
				Usage:       "Cluster Sync",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate sync <cluster> <schema>")
					}

					config := kw.Parse("Kwfile.yml")
					cluster := ctx.Args().Get(0)
					lists, ok := config.Migrate.Cluster[cluster]
					if !ok {
						return errors.New(fmt.Sprintf("Cluster '%s' isn't defined", cluster))
					}

					connections := map[string]kw.Connection{}
					for _, c := range lists {
						if config.Migrate.Source == c {
							continue
						}

						if _, ok := config.Migrate.Connections[c]; !ok {
							return errors.New(fmt.Sprintf("Connection '%s' isn't defined", c))
						}

						connections[c] = config.Migrate.Connections[c]
					}

					schema := ctx.Args().Get(1)
					for i, source := range connections {
						db, err := kw.Connect(source)
						if err != nil {
							return err
						}

						migrator := migrate.NewMigrator(db, source.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

						progress := spinner.New(spinner.CharSets[spinerIndex], duration)
						progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", i, schema)
						progress.Start()

						err = migrator.Up()
						if err != nil {
							if err != gomigrate.ErrNoChange {
								version, dirty, _ := migrator.Version()
								if version != 0 && dirty {
									migrator.Force(int(version))
									migrator.Steps(-1)
								}
							}

							progress.Stop()

							return err
						}

						progress.Stop()
					}

					return nil
				},
			},
			{
				Name:        "up",
				Aliases:     []string{"u"},
				Description: "up [<db>] [<schema>] [--all-connection] [--all-schema]",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "all-connection", Aliases: []string{"ac"}},
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

								migrator := migrate.NewMigrator(db, source.Name, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
								err := migrator.Up()
								if err != nil {
									if err != gomigrate.ErrNoChange {
										version, dirty, _ := migrator.Version()
										if version != 0 && dirty {
											migrator.Force(int(version))
											migrator.Steps(-1)
										}
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

							migrator := migrate.NewMigrator(db, dbConfig.Name, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
							err := migrator.Up()
							if err != nil {
								if err != gomigrate.ErrNoChange {
									version, dirty, _ := migrator.Version()
									if version != 0 && dirty {
										migrator.Force(int(version))
										migrator.Steps(-1)
									}
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

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", source, schema)
					progress.Start()

					err = migrator.Up()
					if err != nil {
						if err != gomigrate.ErrNoChange {
							version, dirty, _ := migrator.Version()
							if version != 0 && dirty {
								fmt.Println("HE")
								migrator.Force(int(version))
								err := migrator.Steps(-1)

								fmt.Println(err)
							}
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

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Steps(int(n))
				},
			},
			{
				Name:        "run",
				Aliases:     []string{"s"},
				Description: "run <db> <schema> <step>",
				Usage:       "Run Migration",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					if ctx.NArg() != 3 {
						return errors.New("Not enough arguments. Usage: kw-migrate run <db> <schema> <step>")
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
					if err != nil || n <= 0 {
						return errors.New("Invalid step")
					}

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

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

								migrator := migrate.NewMigrator(db, source.Name, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
								err := migrator.Down()
								if err != nil {
									if err != gomigrate.ErrNoChange {
										version, dirty, _ := migrator.Version()
										if version != 0 && dirty {
											migrator.Force(int(version))

											migrator.Steps(-1)
										}
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
							progress := spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", source, k)
							progress.Start()

							migrator := migrate.NewMigrator(db, dbConfig.Name, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
							err := migrator.Down()
							if err != nil {
								if err != gomigrate.ErrNoChange {
									version, dirty, _ := migrator.Version()
									if version != 0 && dirty {
										migrator.Force(int(version))
										migrator.Steps(-1)
									}
								}

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

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", source, schema)
					progress.Start()

					err = migrator.Down()
					if err != nil {
						if err != gomigrate.ErrNoChange {
							version, dirty, _ := migrator.Version()
							if version != 0 && dirty {
								migrator.Force(int(version))
								migrator.Steps(-1)
							}
						}
					}

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

						os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, schema), 0777)

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

							script := ddl.Generate(fmt.Sprintf("%s.%s", schema, t), schemaOnly)

							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, schema, version, t), []byte(script.UpScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, schema, version, t), []byte(script.DownScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", config.Migrate.Folder, schema, version, t), []byte(script.UpReferenceScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", config.Migrate.Folder, schema, version, t), []byte(script.DownReferenceScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", config.Migrate.Folder, schema, version, t), []byte(script.UpForeignScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", config.Migrate.Folder, schema, version, t), []byte(script.DownForeignScript), 0777)
							if err != nil {
								progress.Stop()

								return err
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
					for k, v := range config.Migrate.Schemas {
						os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, k), 0777)

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

							script := ddl.Generate(fmt.Sprintf("%s.%s", schema, t), schemaOnly)

							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, k, version, t), []byte(script.UpScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, k, version, t), []byte(script.DownScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", config.Migrate.Folder, k, version, t), []byte(script.UpReferenceScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", config.Migrate.Folder, k, version, t), []byte(script.DownReferenceScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", config.Migrate.Folder, k, version, t), []byte(script.UpForeignScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", config.Migrate.Folder, k, version, t), []byte(script.DownForeignScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}
						}

						i++
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
