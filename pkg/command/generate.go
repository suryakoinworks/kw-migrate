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
	progress.Suffix = fmt.Sprintf(" Listing tables on schema %s... ", schema)
	progress.Start()

	_, ok := g.config.Schemas[schema]
	if !ok {
		g.errorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	os.MkdirAll(fmt.Sprintf("%s/%s", g.config.Folder, schema), 0777)

	version := time.Now().Unix()

	udts := db.NewEnum(g.connection).GenerateDdl(schema)
	for _, s := range udts {
		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_enum_%s.up.sql", g.config.Folder, schema, version, s.Name), []byte(s.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_enum_%s.down.sql", g.config.Folder, schema, version, s.Name), []byte(s.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		version++
	}

	functions := db.NewFunction(g.connection).GenerateDdl(schema)
	for _, s := range functions {
		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_function_%s.up.sql", g.config.Folder, schema, version, s.Name), []byte(s.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_function_%s.down.sql", g.config.Folder, schema, version, s.Name), []byte(s.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		version++
	}

	views := db.NewView(g.connection).GenerateDdl(schema)
	for _, s := range views {
		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_view_%s.up.sql", g.config.Folder, schema, version, s.Name), []byte(s.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_view_%s.down.sql", g.config.Folder, schema, version, s.Name), []byte(s.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		version++
	}

	mViews := db.NewView(g.connection).GenerateDdl(schema)
	for _, s := range mViews {
		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.up.sql", g.config.Folder, schema, version, s.Name), []byte(s.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.down.sql", g.config.Folder, schema, version, s.Name), []byte(s.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

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

	tlen := len(g.config.Schemas[schema]["tables"])
	for j, t := range g.config.Schemas[schema]["tables"] {
		progress.Stop()
		progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.Suffix = fmt.Sprintf(" Processing table %s (%d/%d) on schema %s... ", g.successColor.Sprint(t), (j + 1), tlen, g.successColor.Sprint(schema))
		progress.Start()

		schemaOnly := true
		for _, d := range g.config.Schemas[schema]["with_data"] {
			if d == t {
				schemaOnly = false

				break
			}
		}

		script := ddlTool.Generate(fmt.Sprintf("%s.%s", schema, t), schemaOnly)
		scripts[schema][script.Name] = script

		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", g.config.Folder, schema, version, t), []byte(script.Definition.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", g.config.Folder, schema, version, t), []byte(script.Definition.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		version++

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", g.config.Folder, schema, version, t), []byte(script.Reference.UpScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", g.config.Folder, schema, version, t), []byte(script.Reference.DownScript), 0777)
		if err != nil {
			progress.Stop()

			g.errorColor.Println(err.Error())

			return nil
		}

		version++
	}

	progress.Stop()
	progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Mapping references for schema %s...", g.successColor.Sprint(schema))
	progress.Start()

	for k, s := range scripts {
		for _, c := range s {
			if c.ForeignKey.UpScript == "" {
				continue
			}

			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.up.sql", g.config.Folder, k, version, c.Name), []byte(c.ForeignKey.UpScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return nil
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_keys_%s.down.sql", g.config.Folder, k, version, c.Name), []byte(c.ForeignKey.DownScript), 0777)
			if err != nil {
				progress.Stop()

				g.errorColor.Println(err.Error())

				return nil
			}

			version++
		}

	}

	progress.Stop()

	g.successColor.Printf("Migration generation on schema %s run successfully\n", schema)

	return nil
}
