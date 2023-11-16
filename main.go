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
	"github.com/urfave/cli/v2"
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
				Name:        "version",
				Aliases:     []string{"version"},
				Description: "version",
				Usage:       "Show kmt version",
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
