package command

import (
	"database/sql"
	"fmt"
	"kmt/pkg/config"
	"kmt/pkg/db"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
)

type generate struct {
	config       config.Migration
	connection   *sql.DB
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewGenerate(config config.Migration, connection *sql.DB) generate {
	return generate{
		config:       config,
		connection:   connection,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (g generate) Call(schema string) error {
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Listing tables on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	source, ok := g.config.Connections[g.config.Source]
	if !ok {
		g.errorColor.Printf("Config for '%s' not found", g.boldFont.Sprint(g.config.Source))

		return nil
	}

	schemaConfig, ok := source.Schemas[schema]
	if !ok {
		g.errorColor.Printf("Schema '%s' not found\n", g.boldFont.Sprint(schema))

		return nil
	}

	os.MkdirAll(fmt.Sprintf("%s/%s", g.config.Folder, schema), 0777)

	version := time.Now().Unix()

	progress.Stop()
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", g.successColor.Sprint(schema))

	udts := db.NewEnum(g.connection).GenerateDdl(schema)
	for s := range udts {
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_enum_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_enum_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}
		}(version, schema, s)

		version++
	}

	progress.Stop()

	schemaTool := db.NewSchema(g.connection)
	cTable := schemaTool.ListTable(schema, schemaConfig["excludes"]...)

	ddlTool := db.NewTable(g.config.PgDump, source)
	cDdl := make(chan db.Ddl)
	tTable := schemaTool.CountTable(schema, len(schemaConfig["excludes"]))

	go func(version int64, schema string, tTable int, cDdl chan<- db.Ddl, cTable <-chan string) {
		count := 1
		for tableName := range cTable {
			progress.Stop()
			progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
			progress.Suffix = fmt.Sprintf(" Processing table %s (%d/%d) on schema %s...", g.successColor.Sprint(tableName), count, tTable, g.successColor.Sprint(schema))
			progress.Start()

			schemaOnly := true
			for _, d := range schemaConfig["with_data"] {
				if d == tableName {
					schemaOnly = false

					break
				}
			}

			script := ddlTool.Generate(fmt.Sprintf("%s.%s", schema, tableName), schemaOnly)

			cDdl <- script

			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", g.config.Folder, schema, version, tableName), []byte(script.Definition.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", g.config.Folder, schema, version, tableName), []byte(script.Definition.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			version++

			if script.Reference.UpScript == "" {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", g.config.Folder, schema, version, tableName), []byte(script.Reference.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", g.config.Folder, schema, version, tableName), []byte(script.Reference.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			version++
			count++
		}

		close(cDdl)
	}(version, schema, tTable, cDdl, cTable)

	version = (version - 4) + int64(tTable*2)

	for ddl := range cDdl {
		if ddl.ForeignKey.UpScript == "" {
			continue
		}

		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.ForeignKey.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			continue
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.ForeignKey.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			continue
		}

		version++
	}

	progress.Stop()
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", g.successColor.Sprint(schema))

	functions := db.NewFunction(g.connection).GenerateDdl(schema)
	for s := range functions {
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_function_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_function_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}
		}(version, schema, s)

		version++
	}

	progress.Stop()
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", g.successColor.Sprint(schema))

	views := db.NewView(g.connection).GenerateDdl(schema)
	for s := range views {
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_view_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_view_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}
		}(version, schema, s)

		version++
	}

	progress.Stop()
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", g.successColor.Sprint(schema))

	mViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
	for s := range mViews {
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return
			}
		}(version, schema, s)

		version++
	}

	g.successColor.Printf("Migration generation on schema %s run successfully\n", g.boldFont.Sprint(schema))

	return nil
}
