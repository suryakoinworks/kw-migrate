package main

import (
	"errors"
	"fmt"
	kmt "kmt/config"
	"kmt/migrate"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	gomigrate "github.com/golang-migrate/migrate/v4"
	"github.com/urfave/cli/v2"
)

const (
	VERSION_MAJOR  = 10000
	VERSION_MINOR  = 100
	VERSION_PATCH  = 11
	VERSION_STRING = "1.1.11"
)

var (
	spinerIndex = 9
	duration    = 77 * time.Millisecond
)

func main() {
	app := &cli.App{
		Name:                   "kmt",
		Usage:                  "Koinworks Migration Tool",
		Description:            "kmt help",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Commands: []*cli.Command{
			{
				Name:        "sync",
				Aliases:     []string{"syc"},
				Description: "sync <cluster> <schema>",
				Usage:       "Cluster Sync",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt sync <cluster> <schema>")
					}

					config := kmt.Parse("Kwfile.yml")
					cluster := ctx.Args().Get(0)
					lists, ok := config.Migrate.Cluster[cluster]
					if !ok {
						return fmt.Errorf("cluster '%s' isn't defined", cluster)
					}

					connections := map[string]kmt.Connection{}
					for _, c := range lists {
						if config.Migrate.Source == c {
							continue
						}

						if _, ok := config.Migrate.Connections[c]; !ok {
							return fmt.Errorf("connection '%s' isn't defined", c)
						}

						connections[c] = config.Migrate.Connections[c]
					}

					schema := ctx.Args().Get(1)
					for i, source := range connections {
						db, err := kmt.Connect(source)
						if err != nil {
							return err
						}

						migrator := migrate.NewMigrator(db, source.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

						progress := spinner.New(spinner.CharSets[spinerIndex], duration)
						progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", i, schema)
						progress.Start()

						err = migrator.Up()
						if err == nil || err == gomigrate.ErrNoChange {
							progress.Stop()

							return nil
						}

						version, dirty, _ := migrator.Version()
						if version != 0 && dirty {
							migrator.Force(int(version))
							migrator.Steps(-1)
						}

						progress.Stop()
					}

					return nil
				},
			},
			{
				Name:        "up",
				Aliases:     []string{"u"},
				Description: "up [<db>] [<schema>]",
				Usage:       "Migration Up",
				Action: func(ctx *cli.Context) error {
					config := kmt.Parse("Kwfile.yml")
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt up <db> <schema>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						source := ctx.Args().Get(0)
						return fmt.Errorf("database connection '%s' not found", source)
					}

					db, err := kmt.Connect(dbConfig)
					if err != nil {
						return err
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return fmt.Errorf("schema '%s' not found", schema)
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
					if err == nil || err == gomigrate.ErrNoChange {
						progress.Stop()

						return nil
					}

					version, dirty, _ := migrator.Version()
					if version != 0 && dirty {
						migrator.Force(int(version))
						migrator.Steps(-1)
					}

					progress.Stop()

					return err
				},
			},
			{
				Name:        "rollback",
				Aliases:     []string{"rb"},
				Description: "rollback <db> <schema> <step>",
				Usage:       "Migration Rollback",
				Action: func(ctx *cli.Context) error {
					config := kmt.Parse("Kwfile.yml")
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt rollback <db> <schema> <step>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						return fmt.Errorf("database connection '%s' not found", source)
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return fmt.Errorf("schema '%s' not found", schema)
					}

					db, err := kmt.Connect(dbConfig)
					if err != nil {
						return err
					}

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil || n <= 0 {
						return errors.New("invalid step")
					}

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Steps(int(n * -1))
				},
			},
			{
				Name:        "run",
				Aliases:     []string{"rn"},
				Description: "run <db> <schema> <step>",
				Usage:       "Run Migration",
				Action: func(ctx *cli.Context) error {
					config := kmt.Parse("Kwfile.yml")
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt run <db> <schema> <step>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						return fmt.Errorf("database connection '%s' not found", source)
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return fmt.Errorf("schema '%s' not found", schema)
					}

					db, err := kmt.Connect(dbConfig)
					if err != nil {
						return err
					}

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil || n <= 0 {
						return errors.New("invalid step")
					}

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Steps(int(n))
				},
			},
			{
				Name:    "down",
				Aliases: []string{"dwn"},
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "all-connection", Aliases: []string{"ac"}},
					&cli.BoolFlag{Name: "all-schema", Aliases: []string{"as"}},
				},
				Description: "down [<db>] [<schema>] [--all-connection] [--all-schema]",
				Usage:       "Migration Down",
				Action: func(ctx *cli.Context) error {
					config := kmt.Parse("Kwfile.yml")
					if ctx.Bool("all-connection") {
						for i, source := range config.Migrate.Connections {
							if config.Migrate.Source == i {
								continue
							}

							db, err := kmt.Connect(source)
							if err != nil {
								return err
							}

							for k := range config.Migrate.Schemas {
								progress := spinner.New(spinner.CharSets[spinerIndex], duration)
								progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", i, k)
								progress.Start()

								migrator := migrate.NewMigrator(db, source.Name, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
								err = migrator.Down()
								if err == nil || err == gomigrate.ErrNoChange {
									progress.Stop()

									return nil
								}

								version, dirty, _ := migrator.Version()
								if version != 0 && dirty {
									migrator.Force(int(version))
									migrator.Steps(-1)
								}

								progress.Stop()
							}
						}

						return nil
					}

					if ctx.Bool("all-schema") {
						if ctx.NArg() != 1 {
							return errors.New("not enough arguments. Usage: kmt up <db> --all-schema")
						}

						source := ctx.Args().Get(0)
						dbConfig, ok := config.Migrate.Connections[source]
						if !ok {
							return fmt.Errorf("database connection '%s' not found", source)
						}

						db, err := kmt.Connect(dbConfig)
						if err != nil {
							return err
						}

						for k := range config.Migrate.Schemas {
							progress := spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", source, k)
							progress.Start()

							migrator := migrate.NewMigrator(db, dbConfig.Name, k, fmt.Sprintf("%s/%s", config.Migrate.Folder, k))
							err = migrator.Down()
							if err == nil || err == gomigrate.ErrNoChange {
								progress.Stop()

								return nil
							}

							version, dirty, _ := migrator.Version()
							if version != 0 && dirty {
								migrator.Force(int(version))
								migrator.Steps(-1)
							}

							progress.Stop()
						}

						return nil
					}

					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt down <db> <schema>")
					}

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						return fmt.Errorf("database connection '%s' not found", source)
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return fmt.Errorf("schema '%s' not found", schema)
					}

					db, err := kmt.Connect(dbConfig)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", source, schema)
					progress.Start()

					err = migrator.Down()
					if err == nil || err == gomigrate.ErrNoChange {
						progress.Stop()

						return nil
					}

					version, dirty, _ := migrator.Version()
					if version != 0 && dirty {
						migrator.Force(int(version))
						migrator.Steps(-1)
					}

					progress.Stop()

					return err
				},
			},
			{
				Name:        "clean",
				Aliases:     []string{"cln"},
				Description: "clean <db> <schema>",
				Usage:       "Clean dirty migration",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt clean <db> <schema>")
					}

					config := kmt.Parse("Kwfile.yml")

					source := ctx.Args().Get(0)
					dbConfig, ok := config.Migrate.Connections[source]
					if !ok {
						return fmt.Errorf("database connection '%s' not found", source)
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return fmt.Errorf("schema '%s' not found", schema)
					}

					db, err := kmt.Connect(dbConfig)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					version, dirty, _ := migrator.Version()
					if version != 0 && dirty {
						migrator.Force(int(version))
						migrator.Steps(-1)
					}

					return err
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"crt"},
				Description: "create <schema> <name>",
				Usage:       "Create New Migration  for Schema",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt create <schema> <name>")
					}

					config := kmt.Parse("Kwfile.yml")

					schema := ctx.Args().Get(0)
					_, ok := config.Migrate.Schemas[schema]
					if !ok {
						return fmt.Errorf("schema '%s' not found", schema)
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
				Description: "generate <schema>",
				Usage:       "Generate Migration from Existing Database",
				Action: func(ctx *cli.Context) error {
					config := kmt.Parse("Kwfile.yml")
					source, ok := config.Migrate.Connections[config.Migrate.Source]
					if !ok {
						return fmt.Errorf("config for '%s' not found", config.Migrate.Source)
					}

					db, err := kmt.Connect(source)
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
							return errors.New("schema not found")
						}

						os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, schema), 0777)

						version := time.Now().Unix()
						schemaTool := migrate.NewSchema(db, schema)

						udts := schemaTool.ListUDT()
						for _, s := range udts {
							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, schema, version, s.Name), []byte(s.UpScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, schema, version, s.Name), []byte(s.DownScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
						}

						functions := schemaTool.ListFunction()
						for _, s := range functions {
							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, schema, version, s.Name), []byte(s.UpScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, schema, version, s.Name), []byte(s.DownScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
						}

						config.Migrate.Schemas[schema]["tables"] = schemaTool.ListTables(config.Migrate.Schemas[schema]["excludes"])

						ddlTool := migrate.NewDdl(config.Migrate.PgDump, source)

						scripts := map[string]map[string]migrate.Script{}
						scripts[schema] = map[string]migrate.Script{}

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

							script := ddlTool.Generate(fmt.Sprintf("%s.%s", schema, t), schemaOnly)
							scripts[schema][script.Table] = script

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
						}

						progress.Stop()
						progress = spinner.New(spinner.CharSets[spinerIndex], duration)
						progress.Suffix = " Mapping references..."
						progress.Start()

						for k, s := range scripts {
							for _, c := range s {
								if c.UpForeignScript == "" {
									continue
								}

								err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", config.Migrate.Folder, k, version, c.Table), []byte(c.UpForeignScript), 0777)
								if err != nil {
									progress.Stop()

									return err
								}

								err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", config.Migrate.Folder, k, version, c.Table), []byte(c.DownForeignScript), 0777)
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
					version := time.Now().Unix()

					for k, v := range config.Migrate.Schemas {
						os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, k), 0777)

						schemaTool := migrate.NewSchema(db, k)

						udts := schemaTool.ListUDT()
						for _, s := range udts {
							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, k, version, s.Name), []byte(s.UpScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, k, version, s.Name), []byte(s.DownScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
						}

						functions := schemaTool.ListFunction()
						for _, s := range functions {
							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, k, version, s.Name), []byte(s.UpScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, k, version, s.Name), []byte(s.DownScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							version++
						}

						config.Migrate.Schemas[k]["tables"] = migrate.NewSchema(db, k).ListTables(v["excludes"])
					}

					progress.Stop()
					progress = spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Generating migration files... "
					progress.Start()

					ddlTool := migrate.NewDdl(config.Migrate.PgDump, source)
					slen := len(config.Migrate.Schemas)
					i := 1

					scripts := map[string]map[string]migrate.Script{}
					for k, v := range config.Migrate.Schemas {
						scripts[k] = map[string]migrate.Script{}
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

							script := ddlTool.Generate(fmt.Sprintf("%s.%s", k, t), schemaOnly)
							scripts[k][script.Table] = script

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
						}

						i++
					}

					progress.Stop()
					progress = spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Mapping references..."
					progress.Start()

					for k, s := range scripts {
						for _, c := range s {
							if c.UpForeignScript == "" {
								continue
							}

							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", config.Migrate.Folder, k, version, c.Table), []byte(c.UpForeignScript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", config.Migrate.Folder, k, version, c.Table), []byte(c.DownForeignScript), 0777)
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
			{
				Name:        "test",
				Aliases:     []string{"test"},
				Description: "test",
				Usage:       "Test kmt configuration",
				Action: func(ctx *cli.Context) error {
					config := kmt.Parse("Kwfile.yml")

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Test connections config..."
					progress.Start()

					for i, c := range config.Migrate.Connections {
						progress.Stop()
						progress.Suffix = fmt.Sprintf(" Test connection to %s...", i)
						progress.Start()

						_, err := kmt.Connect(c)
						if err != nil {
							progress.Stop()
							progress.Suffix = fmt.Sprintf(" Unable to connect to %s...", color.New(color.FgRed).Sprint(i))
							progress.Start()

							return err
						}
					}

					progress.Stop()

					progress = spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Test 'pg_dump' command..."
					progress.Start()

					cli := exec.Command(config.Migrate.PgDump, "--help")
					_, err := cli.CombinedOutput()
					if err != nil {
						progress.Stop()
						progress.Suffix = fmt.Sprintf(" 'pg_dump' not found on %s...", color.New(color.FgRed).Sprint(config.Migrate.PgDump))
						progress.Start()

						return fmt.Errorf("'pg_dump' not found on %s", config.Migrate.PgDump)
					}

					progress.Stop()

					color.New(color.FgGreen).Println("Config test passed")

					return nil
				},
			},
			{
				Name:        "version",
				Aliases:     []string{"version"},
				Description: "version",
				Usage:       "Show kmt version",
				Action: func(ctx *cli.Context) error {
					color.New(color.FgGreen).Printf("VersionID: %d\n", VERSION_MAJOR+VERSION_MINOR+VERSION_PATCH)
					color.New(color.FgGreen).Printf("Version: %s\n\n", VERSION_STRING)
					color.New(color.FgGreen).Println("Author: Muhamad Surya Iksanudin<surya.iksanudi@koinworks.com>")

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
