package migrate

import (
	"database/sql"
	"fmt"
	"kmt/config"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type (
	Script struct {
		Table               string
		UpScript            string
		UpReferenceScript   string
		UpForeignScript     string
		DownScript          string
		DownReferenceScript string
		DownForeignScript   string
	}

	udt struct {
		Name       string
		UpScript   string
		DownScript string
	}

	function struct {
		Name       string
		UpScript   string
		DownScript string
	}

	schema struct {
		db   *sql.DB
		name string
	}

	ddl struct {
		command string
		config  config.Connection
	}
)

func NewDdl(command string, config config.Connection) ddl {
	return ddl{command: command, config: config}
}

func NewSchema(db *sql.DB, name string) schema {
	return schema{db: db, name: name}
}

func (d ddl) Generate(table string, schemaOnly bool) Script {
	options := []string{
		"--no-comments",
		"--no-publications",
		"--no-security-labels",
		"--no-subscriptions",
		"--no-synchronized-snapshots",
		"--no-tablespaces",
		"--no-unlogged-table-data",
		"--no-owner",
		"--no-privileges",
		"--no-blobs",
		"--clean",
		"--username", d.config.User,
		"--port", strconv.Itoa(d.config.Port),
		"--host", d.config.Host,
		"--table", table,
		d.config.Name,
	}

	if schemaOnly {
		options = append(options, "--schema-only")
	} else {
		options = append(options, "--inserts")
	}

	cli := exec.Command(d.command, options...)
	cli.Env = os.Environ()
	cli.Env = append(cli.Env, fmt.Sprintf("PGPASSWORD=%s", d.config.Password))

	var upScript []string
	var downScript []string
	var upReferenceScript []string
	var downReferenceScript []string
	var upForeignScript []string
	var downForeignScript []string
	var skip bool = false

	result, _ := cli.CombinedOutput()
	lines := strings.Split(string(result), "\n")
	for n, line := range lines {
		if d.skip(line) || skip {
			skip = false

			continue
		}

		if d.downScript(line) {
			if d.downReferenceScript(line) {
				if d.downForeignkey(line) {
					downForeignScript = append(downForeignScript, line)
				} else {
					downReferenceScript = append(downReferenceScript, line)
				}
			} else {
				downScript = append(downScript, line)
			}
		} else {
			if d.refereceScript(line, n, lines) {
				if d.foreignScript(lines[n+1]) {
					upForeignScript = append(upForeignScript, line)
					upForeignScript = append(upForeignScript, lines[n+1])
				} else {
					upReferenceScript = append(upReferenceScript, line)
					upReferenceScript = append(upReferenceScript, lines[n+1])
				}
				skip = true
			} else {
				upScript = append(upScript, line)
			}
		}
	}

	return Script{
		Table:               strings.ReplaceAll(table, ".", "_"),
		UpScript:            strings.Join(upScript, "\n"),
		UpReferenceScript:   strings.Join(upReferenceScript, "\n"),
		UpForeignScript:     strings.Join(upForeignScript, "\n"),
		DownScript:          strings.Join(downScript, "\n"),
		DownReferenceScript: strings.Join(downReferenceScript, "\n"),
		DownForeignScript:   strings.Join(downForeignScript, "\n"),
	}
}

func (d ddl) skip(line string) bool {
	return line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET ") || strings.HasPrefix(line, "SELECT ")
}

func (d ddl) downScript(line string) bool {
	return strings.Contains(line, "DROP")
}

func (d ddl) downReferenceScript(line string) bool {
	return strings.Contains(line, "pkey") || strings.Contains(line, "fkey") || strings.Contains(line, "pk") || strings.Contains(line, "fk")
}

func (d ddl) downForeignkey(line string) bool {
	return strings.Contains(line, "fkey") || strings.Contains(line, "fk")
}

func (d ddl) foreignScript(line string) bool {
	return strings.Contains(line, "FOREIGN KEY")
}

func (d ddl) refereceScript(line string, n int, lines []string) bool {
	return strings.Contains(line, "ALTER TABLE ONLY") && strings.Contains(lines[n+1], "ADD CONSTRAINT")
}

func (s schema) ListTables(excludes []string) []string {
	query := fmt.Sprintf("SELECT LOWER(table_name) as table_name FROM information_schema.tables WHERE table_schema='%s' AND table_type='BASE TABLE' ORDER BY table_name;", s.name)
	rows, err := s.db.Query(query)
	if err != nil {
		fmt.Println(err.Error())

		return []string{}
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		skip := false
		for _, v := range excludes {
			if v == table {
				skip = true

				break
			}
		}

		if !skip {
			tables = append(tables, table)
			skip = false
		}
	}

	return tables
}

func (s schema) ListFunction() []function {
	query := fmt.Sprintf(`SELECT
    p.proname AS function_name,
    pg_get_functiondef(p.oid) AS function_definition
FROM pg_proc p
JOIN pg_namespace n
    ON n.oid = p.pronamespace
WHERE n.nspname = '%s';`, s.name)

	rows, err := s.db.Query(query)
	if err != nil {
		fmt.Println(err.Error())

		return []function{}
	}
	defer rows.Close()

	functions := []function{}
	for rows.Next() {
		var name string
		var definition string
		err = rows.Scan(&name, &definition)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		functions = append(functions, function{
			Name:       name,
			UpScript:   definition,
			DownScript: "",
		})
	}

	return functions
}

func (s schema) ListUDT() []udt {
	query := fmt.Sprintf(`SELECT
    pg_catalog.format_type ( t.oid, NULL ) AS name,
    pg_catalog.array_to_string (
        ARRAY( SELECT e.enumlabel
                FROM pg_catalog.pg_enum e
                WHERE e.enumtypid = t.oid
                ORDER BY e.oid ), '#'
        ) AS values
FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n
    ON n.oid = t.typnamespace
WHERE ( t.typrelid = 0
        OR ( SELECT c.relkind = 'c'
                FROM pg_catalog.pg_class c
                WHERE c.oid = t.typrelid
            )
    )
    AND NOT EXISTS
        ( SELECT 1
            FROM pg_catalog.pg_type el
            WHERE el.oid = t.typelem
                AND el.typarray = t.oid
        )
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname = '%s'
ORDER BY name;`, s.name)

	rows, err := s.db.Query(query)
	if err != nil {
		fmt.Println(err.Error())

		return []udt{}
	}
	defer rows.Close()

	udts := []udt{}
	for rows.Next() {
		var name string
		var values string
		err = rows.Scan(&name, &values)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		udts = append(udts, udt{
			Name:       strings.Replace(name, "master.", "", 1),
			UpScript:   s.generateUdtDdl(name, values),
			DownScript: fmt.Sprintf("DROP TYPE %s;", name),
		})
	}

	return udts
}

func (s schema) generateUdtDdl(udt string, values string) string {
	ddl := fmt.Sprintf("CREATE TYPE %s AS ENUM (", udt)

	for _, s := range strings.Split(values, "#") {
		ddl = fmt.Sprintf("%s '%s',", ddl, s)
	}

	ddl = strings.TrimRight(ddl, ",")
	ddl = fmt.Sprintf("%s);", ddl)

	return ddl
}
