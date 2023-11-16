package main

import (
	"errors"
	"fmt"
	"kmt/pkg/command"
	"kmt/pkg/config"
	"log"
	"os"
	"strconv"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
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

					config := config.Parse("Kwfile.yml")

					return command.NewSync(config.Migration, color.New(color.FgRed), color.New(color.FgGreen)).Run(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "up",
				Aliases:     []string{"u"},
				Description: "up <db> <schema>",
				Usage:       "Migration Up",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt up <db> <schema>")
					}

					config := config.Parse("Kwfile.yml")

					return command.NewUp(config.Migration, color.New(color.FgRed), color.New(color.FgGreen)).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "rollback",
				Aliases:     []string{"rb"},
				Description: "rollback <db> <schema> <step>",
				Usage:       "Migration Rollback",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt rollback <db> <schema> <step>")
					}

					config := config.Parse("Kwfile.yml")
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Step is not number")

						return nil
					}

					return command.NewRollback(config.Migration, errorColor, color.New(color.FgGreen)).Call(ctx.Args().Get(0), ctx.Args().Get(1), int(n))
				},
			},
			{
				Name:        "run",
				Aliases:     []string{"rn"},
				Description: "run <db> <schema> <step>",
				Usage:       "Run Migration",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt run <db> <schema> <step>")
					}

					config := config.Parse("Kwfile.yml")
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Step is not number")

						return nil
					}

					return command.NewRun(config.Migration, errorColor, color.New(color.FgGreen)).Call(ctx.Args().Get(0), ctx.Args().Get(1), int(n))
				},
			},
			{
				Name:        "down",
				Aliases:     []string{"dwn"},
				Description: "down <db> <schema>",
				Usage:       "Migration Down",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt down <db> <schema>")
					}

					config := config.Parse("Kwfile.yml")

					return command.NewDown(config.Migration, color.New(color.FgRed), color.New(color.FgGreen)).Call(ctx.Args().Get(0), ctx.Args().Get(1))
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

					config := config.Parse("Kwfile.yml")

					return command.NewClean(config.Migration, color.New(color.FgRed), color.New(color.FgGreen)).Call(ctx.Args().Get(0), ctx.Args().Get(1))
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

					config := config.Parse("Kwfile.yml")

					return command.NewClean(config.Migration, color.New(color.FgRed), color.New(color.FgGreen)).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "generate",
				Aliases:     []string{"gen"},
				Description: "generate [<schema>]",
				Usage:       "Generate Migration from Existing Database",
				Action: func(ctx *cli.Context) error {
					cfg := config.Parse("Kwfile.yml")
					source, ok := cfg.Migration.Connections[cfg.Migration.Source]
					if !ok {
						return fmt.Errorf("config for '%s' not found", cfg.Migration.Source)
					}

					db, err := config.NewConnection(source)
					if err != nil {
						return err
					}

					cmd := command.NewGenerate(cfg.Migration, db, color.New(color.FgRed), color.New(color.FgGreen))
					if ctx.NArg() == 1 {
						return cmd.Call(ctx.Args().Get(0))
					}

					for k := range cfg.Migration.Schemas {
						cmd.Call(k)
					}

					return nil
				},
			},
			{
				Name:        "version",
				Aliases:     []string{"version"},
				Description: "version <db> [<schema>]",
				Usage:       "Show migration version",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() < 1 {
						return errors.New("not enough arguments. Usage: kmt version <db> [<schema>]")
					}

					config := config.Parse("Kwfile.yml")
					cmd := command.NewVersion(config.Migration, color.New(color.FgRed), color.New(color.FgGreen))

					t := table.NewWriter()
					t.SetOutputMirror(os.Stdout)
					t.AppendHeader(table.Row{"#", "Connection", "Schema", "Version"})

					if ctx.NArg() == 2 {
						db := ctx.Args().Get(0)
						schema := ctx.Args().Get(1)
						version := cmd.Call(db, schema)

						t.AppendRows([]table.Row{
							{1, db, schema, version},
						})
						t.Render()

						return nil
					}

					db := ctx.Args().Get(0)
					for k := range config.Migration.Schemas {
						version := cmd.Call(db, k)

						t.AppendRows([]table.Row{
							{1, db, k, version},
						})
					}

					t.Render()

					return nil
				},
			},
			{
				Name:        "compare",
				Aliases:     []string{"compare"},
				Description: "compare <source> <compare> [<schema>]",
				Usage:       "Show migration version comparation",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt compare <source> <compare> [<schema>]")
					}

					config := config.Parse("Kwfile.yml")
					cmd := command.NewCompare(config.Migration, color.New(color.FgRed), color.New(color.FgGreen))

					t := table.NewWriter()
					t.SetOutputMirror(os.Stdout)

					c := cases.Title(language.Indonesian)

					source := ctx.Args().Get(0)
					compare := ctx.Args().Get(1)

					t.AppendHeader(table.Row{"#", "Schema", fmt.Sprintf("%s Version", c.String(source)), fmt.Sprintf("%s Version", c.String(compare))})

					if ctx.NArg() == 3 {
						schema := ctx.Args().Get(2)
						vSource, vCompare := cmd.Call(source, compare, schema)

						t.AppendRows([]table.Row{
							{1, schema, vSource, vCompare},
						})
						t.Render()

						return nil
					}

					for k := range config.Migration.Schemas {
						vSource, vCompare := cmd.Call(source, compare, k)

						t.AppendRows([]table.Row{
							{1, k, vSource, vCompare},
						})
					}

					t.Render()

					return nil
				},
			},
			{
				Name:        "test",
				Aliases:     []string{"test"},
				Description: "test",
				Usage:       "Test kmt configuration",
				Action: func(ctx *cli.Context) error {
					config := config.Parse("Kwfile.yml")

					return command.NewTest(config.Migration, color.New(color.FgRed), color.New(color.FgGreen)).Call()
				},
			},
			{
				Name:        "upgrade",
				Aliases:     []string{"upgrade"},
				Description: "upgrade",
				Usage:       "Upgrade kmt to latest version",
				Action: func(ctx *cli.Context) error {
					return command.NewUpgrade(color.New(color.FgRed), color.New(color.FgGreen)).Call()
				},
			},
			{
				Name:        "about",
				Aliases:     []string{"about"},
				Description: "about",
				Usage:       "Show kmt profile",
				Action: func(ctx *cli.Context) error {
					gColor := color.New(color.FgGreen)

					fmt.Printf("VersionID: %s\n", gColor.Sprint(config.VERSION_MAJOR+config.VERSION_MINOR+config.VERSION_PATCH))
					fmt.Printf("Version: %s\n\n", gColor.Sprint(config.VERSION_STRING))
					fmt.Printf("Author: %s<surya.iksanudi@koinworks.com>\n", gColor.Sprint("Muhamad Surya Iksanudin"))

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
