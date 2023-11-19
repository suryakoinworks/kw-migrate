package command

import (
	"database/sql"
	"fmt"
	"kmt/pkg/config"
	"kmt/pkg/db"
	"os"
	intlSync "sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
)

type generate struct {
	config       config.Migration
	connection   *sql.DB
	errorColor   *color.Color
	successColor *color.Color
}

func NewGenerate(config config.Migration, connection *sql.DB, errorColor *color.Color, successColor *color.Color) generate {
	return generate{
		config:       config,
		connection:   connection,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (g generate) Call(schema string) error {
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Listing tables on schema %s...", schema)
	progress.Start()

	_, ok := g.config.Schemas[schema]
	if !ok {
		g.errorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	os.MkdirAll(fmt.Sprintf("%s/%s", g.config.Folder, schema), 0777)

	version := time.Now().Unix()

	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", g.successColor.Sprint(schema))

	udts := db.NewEnum(g.connection).GenerateDdl(schema)
	for _, s := range udts {
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
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", g.successColor.Sprint(schema))

	functions := db.NewFunction(g.connection).GenerateDdl(schema)
	for _, s := range functions {
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
	for _, s := range views {
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

	mViews := db.NewView(g.connection).GenerateDdl(schema)
	for _, s := range mViews {
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

	source, ok := g.config.Connections[g.config.Source]
	if !ok {
		g.errorColor.Printf("Config for '%s' not found", g.config.Source)

		return nil
	}

	g.config.Schemas[schema]["tables"] = db.NewSchema(g.connection).ListTables(schema, g.config.Schemas[schema]["excludes"]...)

	ddlTool := db.NewTable(g.config.PgDump, source)

	scripts := map[string]map[string]db.Ddl{}
	scripts[schema] = map[string]db.Ddl{}
	scriptLock := intlSync.Mutex{}
	waitGroup := intlSync.WaitGroup{}

	for _, t := range g.config.Schemas[schema]["tables"] {
		waitGroup.Add(1)

		go func(version int64, schema string, tableName string, wg *intlSync.WaitGroup) {
			progress.Stop()
			progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
			progress.Suffix = fmt.Sprintf(" Processing table %s on schema %s...", g.successColor.Sprint(tableName), g.successColor.Sprint(schema))
			progress.Start()

			schemaOnly := true
			for _, d := range g.config.Schemas[schema]["with_data"] {
				if d == t {
					schemaOnly = false

					break
				}
			}

			scriptLock.Lock()
			script := ddlTool.Generate(fmt.Sprintf("%s.%s", schema, tableName), schemaOnly)
			scripts[schema][script.Name] = script
			scriptLock.Unlock()

			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", g.config.Folder, schema, version, tableName), []byte(script.Definition.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				wg.Done()

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", g.config.Folder, schema, version, tableName), []byte(script.Definition.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				wg.Done()

				return
			}

			version++

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", g.config.Folder, schema, version, tableName), []byte(script.Reference.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				wg.Done()

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", g.config.Folder, schema, version, tableName), []byte(script.Reference.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				wg.Done()

				return
			}

			wg.Done()
		}(version, schema, t, &waitGroup)

		version += 2
	}

	waitGroup.Wait()

	progress.Stop()
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Mapping references for schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	for k, s := range scripts {
		waitGroup = intlSync.WaitGroup{}

		for _, c := range s {
			waitGroup.Add(1)

			go func(version int64, schema string, ddl db.Ddl, wg *intlSync.WaitGroup) {
				if c.ForeignKey.UpScript == "" {
					wg.Done()

					return
				}

				err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(c.ForeignKey.UpScript), 0777)
				if err != nil {
					progress.Stop()

					g.errorColor.Println(err.Error())

					wg.Done()

					return
				}

				err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(c.ForeignKey.DownScript), 0777)
				if err != nil {
					progress.Stop()

					g.errorColor.Println(err.Error())

					wg.Done()

					return
				}

				wg.Done()
			}(version, k, c, &waitGroup)

			version++
		}
		waitGroup.Wait()
	}

	progress.Stop()

	g.successColor.Printf("Migration generation on schema %s run successfully\n", schema)

	return nil
}
