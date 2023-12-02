package command

import (
	"database/sql"
	"fmt"
	"kmt/pkg/config"
	"kmt/pkg/db"
	"os"
	"slices"
	"strings"
	iSync "sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
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
		wg           *iSync.WaitGroup
		tableTool    db.Table
		folder       string
		version      int64
		schema       string
		schemaConfig map[string][]string
		tables       []string
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
		tableSchemaOnly := []string{}
		tableWithData := []string{}
		for _, t := range m.tables {
			for _, d := range m.schemaConfig["with_data"] {
				var fqtn strings.Builder

				fqtn.WriteString(m.schema)
				fqtn.WriteString(".")
				fqtn.WriteString(t)

				if slices.Contains(m.tables, d) {
					tableWithData = append(tableWithData, fqtn.String())

					continue
				}

				tableSchemaOnly = append(tableSchemaOnly, fqtn.String())
			}
		}

		scriptSchemaOnly := m.tableTool.Generate(tableSchemaOnly, true)
		scriptWithData := m.tableTool.Generate(tableWithData, false)

		for _, ddl := range scriptSchemaOnly {
			cDdl <- ddl

			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", m.folder, m.schema, m.version, ddl.Name), []byte(ddl.Definition.UpScript), 0777)
			if err != nil {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", m.folder, m.schema, m.version, ddl.Name), []byte(ddl.Definition.DownScript), 0777)
			if err != nil {
				continue
			}

			if ddl.Reference.UpScript == "" {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", m.folder, m.schema, m.version+1, ddl.Name), []byte(ddl.Reference.UpScript), 0777)
			if err != nil {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", m.folder, m.schema, m.version+1, ddl.Name), []byte(ddl.Reference.DownScript), 0777)
			if err != nil {
				continue
			}
		}

		for _, ddl := range scriptWithData {
			cDdl <- ddl

			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", m.folder, m.schema, m.version, ddl.Name), []byte(ddl.Definition.UpScript), 0777)
			if err != nil {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", m.folder, m.schema, m.version, ddl.Name), []byte(ddl.Definition.DownScript), 0777)
			if err != nil {
				continue
			}

			if ddl.Reference.UpScript == "" {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", m.folder, m.schema, m.version+1, ddl.Name), []byte(ddl.Reference.UpScript), 0777)
			if err != nil {
				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", m.folder, m.schema, m.version+1, ddl.Name), []byte(ddl.Reference.DownScript), 0777)
			if err != nil {
				continue
			}
		}

		m.wg.Done()
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
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

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

	nWorker := 5
	schemaTool := db.NewSchema(g.connection)
	cTable := schemaTool.ListTable(nWorker, schema, schemaConfig["excludes"]...)

	ddlTool := db.NewTable(g.config.PgDump, source)
	cDdl := make(chan db.Ddl)
	tTable := schemaTool.CountTable(schema, len(schemaConfig["excludes"]))

	go func(version int64, schema string, tTable int, cDdl chan<- db.Ddl, cTable <-chan []string) {
		cMigration := make(chan migration)
		wg := iSync.WaitGroup{}

		for i := 1; i <= nWorker; i++ {
			go do(cMigration, cDdl)
		}

		count := 1
		for tables := range cTable {
			progress.Stop()
			progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
			progress.Suffix = fmt.Sprintf(" Processing table (%s/%s) on schema %s...", g.successColor.Sprint(count), g.successColor.Sprint(tTable), g.successColor.Sprint(schema))
			progress.Start()

			wg.Add(1)

			cMigration <- migration{
				wg:           &wg,
				tableTool:    ddlTool,
				folder:       g.config.Folder,
				version:      version,
				schema:       schema,
				schemaConfig: schemaConfig,
				tables:       tables,
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
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

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
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	wg := iSync.WaitGroup{}

	mViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
	for s := range mViews {
		wg.Add(1)
		go func(version int64, schema string, ddl db.Migration, wg *iSync.WaitGroup) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				g.errorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				g.errorColor.Println(err.Error())

				return
			}

			wg.Done()
		}(version, schema, s, &wg)

		version++
	}

	wg.Wait()

	progress.Stop()

	g.successColor.Printf("Migration generation on schema %s run successfully\n", g.boldFont.Sprint(schema))

	return nil
}
