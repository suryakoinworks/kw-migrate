package command

import (
	"database/sql"
	"fmt"
	"kmt/pkg/config"
	"kmt/pkg/db"
	"os"
	"runtime"
	iSync "sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/sourcegraph/conc/pool"
)

type (
	generate struct {
		config       config.Migration
		connection   *sql.DB
		boldFont     *color.Color
		errorColor   *color.Color
		successColor *color.Color
	}

	migration struct {
		wg         *iSync.WaitGroup
		tableTool  db.Table
		folder     string
		version    int64
		schema     string
		schemaOnly bool
		table      string
	}
)

func NewGenerate(config config.Migration, connection *sql.DB) generate {
	return generate{
		config:       config,
		connection:   connection,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func do(cMigration <-chan migration, cDdl chan<- db.Ddl) {
	for m := range cMigration {
		defer m.wg.Done()
		script := m.tableTool.Generate(fmt.Sprintf("%s.%s", m.schema, m.table), m.schemaOnly)

		cDdl <- script

		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", m.folder, m.schema, m.version, m.table), []byte(script.Definition.UpScript), 0777)
		if err != nil {
			return
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", m.folder, m.schema, m.version, m.table), []byte(script.Definition.DownScript), 0777)
		if err != nil {
			return
		}

		if script.Reference.UpScript == "" {
			return
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", m.folder, m.schema, m.version+1, m.table), []byte(script.Reference.UpScript), 0777)
		if err != nil {
			return
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", m.folder, m.schema, m.version+1, m.table), []byte(script.Reference.DownScript), 0777)
		if err != nil {
			return
		}
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
	nWorker := runtime.NumCPU()
	worker := pool.New().WithMaxGoroutines(nWorker)

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	udts := db.NewEnum(g.connection).GenerateDdl(schema)
	for s := range udts {
		worker.Go(func() {
			func(version int64, schema string, ddl db.Migration) {
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
		})

		version++
	}

	schemaTool := db.NewSchema(g.connection)
	cTable := schemaTool.ListTable(nWorker, schema, schemaConfig["excludes"]...)

	ddlTool := db.NewTable(g.config.PgDump, source)
	cDdl := make(chan db.Ddl)
	tTable := schemaTool.CountTable(schema, len(schemaConfig["excludes"]))

	go func(version int64, schema string, tTable int, cDdl chan<- db.Ddl, cTable <-chan string) {
		cMigration := make(chan migration)
		wg := iSync.WaitGroup{}

		for i := 1; i <= nWorker; i++ {
			go do(cMigration, cDdl)
		}

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

			wg.Add(1)

			cMigration <- migration{
				wg:         &wg,
				tableTool:  ddlTool,
				folder:     g.config.Folder,
				version:    version,
				schema:     schema,
				schemaOnly: schemaOnly,
				table:      tableName,
			}

			version += 2
			count++
		}
		wg.Wait()

		close(cDdl)
	}(version, schema, tTable, cDdl, cTable)

	version = version + int64(tTable*2)

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
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	functions := db.NewFunction(g.connection).GenerateDdl(schema)
	for s := range functions {
		worker.Go(func() {
			func(version int64, schema string, ddl db.Migration) {
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
		})

		version++
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	views := db.NewView(g.connection).GenerateDdl(schema)
	for s := range views {
		worker.Go(func() {
			func(version int64, schema string, ddl db.Migration) {
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
		})

		version++
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	mViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
	for s := range mViews {
		worker.Go(func() {
			func(version int64, schema string, ddl db.Migration) {
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
		})

		version++
	}

	worker.Wait()
	progress.Stop()

	g.successColor.Printf("Migration generation on schema %s run successfully\n", g.boldFont.Sprint(schema))

	return nil
}
