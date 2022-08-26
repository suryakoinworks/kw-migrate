package migrate

import (
	"database/sql"
	"fmt"
	"koin-migrate/kw"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type (
	schema struct {
		db   *sql.DB
		name string
	}

	ddl struct {
		command string
		config  kw.Connection
	}
)

func NewDdl(command string, config kw.Connection) ddl {
	return ddl{command: command, config: config}
}

func NewSchema(db *sql.DB, name string) schema {
	return schema{db: db, name: name}
}

func (d ddl) Generate(table string, schemaOnly bool) (string, string, string, string) {
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
	var refereceScript []string
	var foreignScript []string
	var skip bool = false

	result, _ := cli.CombinedOutput()
	lines := strings.Split(string(result), "\n")
	for n, line := range lines {
		if d.skip(line) || skip {
			skip = false

			continue
		}

		if d.downScript(line) {
			downScript = append(downScript, line)
		} else {
			if d.refereceScript(line, n, lines) {
				if d.foreignScript(lines[n+1]) {
					foreignScript = append(foreignScript, line)
					foreignScript = append(foreignScript, lines[n+1])
				} else {
					refereceScript = append(refereceScript, line)
					refereceScript = append(refereceScript, lines[n+1])
				}
				skip = true
			} else {
				upScript = append(upScript, line)
			}
		}
	}

	return strings.Join(upScript, "\n"), strings.Join(downScript, "\n"), strings.Join(foreignScript, "\n"), strings.Join(refereceScript, "\n")
}

func (d ddl) skip(line string) bool {
	return line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET ") || strings.HasPrefix(line, "SELECT ")
}

func (d ddl) downScript(line string) bool {
	return strings.Contains(line, "DROP")
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
