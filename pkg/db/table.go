package db

import (
	"fmt"
	"kmt/pkg/config"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

type (
	Table struct {
		command string
		config  config.Connection
	}

	Ddl struct {
		Name       string
		Definition Migration
		Reference  Migration
		ForeignKey Migration
	}
)

func NewTable(command string, config config.Connection) Table {
	return Table{command: command, config: config}
}

func (t Table) Generate(tables []string, schemaOnly bool) map[string]Ddl {
	options := []string{
		"--no-comments",
		"--no-publications",
		"--no-security-labels",
		"--no-subscriptions",
		"--no-synchronized-snapshots",
		"--no-tablespaces",
		"--no-unlogged-table-data",
		"--no-owner",
		"--if-exists",
		"--no-privileges",
		"--no-blobs",
		"--clean",
		"--username", t.config.User,
		"--port", strconv.Itoa(t.config.Port),
		"--host", t.config.Host,
		t.config.Name,
	}

	mapDdl := map[string]Ddl{}
	for _, t := range tables {
		options = append(options, "--table", t)
		mapDdl[t] = Ddl{}
	}

	if schemaOnly {
		options = append(options, "--schema-only")
	} else {
		options = append(options, "--inserts")
	}

	cli := exec.Command(t.command, options...)
	cli.Env = os.Environ()
	cli.Env = append(cli.Env, fmt.Sprintf("PGPASSWORD=%s", t.config.Password))

	var skip bool = false

	result, _ := cli.CombinedOutput()

	fmt.Println(string(result))

	tableIndex := map[string]int{}
	lines := strings.Split(string(result), "\n")
	for n, line := range lines {
		if t.skip(line) || skip {
			continue
		}

		for k := range mapDdl {
			var table strings.Builder

			table.WriteString(CREATE_TABLE)
			table.WriteString(" ")
			table.WriteString(k)
			table.WriteString(" (")

			if strings.Contains(line, k) {
				tableIndex[k] = n

				break
			}
		}
	}

	for x, i := range tableIndex {
		var upScript strings.Builder
		var downScript strings.Builder
		var upReferenceScript strings.Builder
		var downReferenceScript strings.Builder
		var upForeignScript strings.Builder
		var downForeignScript strings.Builder

		for n, line := range lines {
			if n == i {
				break
			}

			if t.skip(line) || skip {
				skip = false

				continue
			}

			if t.downScript(line) {
				if t.downReferenceScript(line) {
					if t.downForeignkey(line) {
						downForeignScript.WriteString(line)
					} else {
						downReferenceScript.WriteString(line)
					}
				} else {
					downScript.WriteString(line)
					downScript.WriteString("\n")
				}
			} else {
				if t.refereceScript(line, n, lines) {
					if t.foreignScript(lines[n+1]) {
						upForeignScript.WriteString(line)
						upForeignScript.WriteString(lines[n+1])
					} else {
						upReferenceScript.WriteString(line)
						upReferenceScript.WriteString("\n")
						upReferenceScript.WriteString(lines[n+1])
						upReferenceScript.WriteString("\n")
					}
					skip = true
				} else {
					upScript.WriteString(line)
					upScript.WriteString("\n")
				}
			}
		}

		mapDdl[x] = Ddl{
			Name: strings.Replace(x, ".", "_", -1),
			Definition: Migration{
				UpScript: strings.Replace(
					strings.Replace(
						strings.Replace(
							upScript.String(),
							CREATE_TABLE,
							SECURE_CREATE_TABLE,
							-1,
						),
						CREATE_SEQUENCE,
						SECURE_CREATE_SEQUENCE,
						-1,
					),
					CREATE_INDEX,
					SECURE_CREATE_INDEX,
					-1,
				),
				DownScript: downScript.String(),
			},
			Reference: Migration{
				UpScript:   upReferenceScript.String(),
				DownScript: downReferenceScript.String(),
			},
			ForeignKey: Migration{
				UpScript:   upForeignScript.String(),
				DownScript: downForeignScript.String(),
			},
		}
	}

	return mapDdl
}

func (Table) skip(line string) bool {
	return line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET ") || strings.HasPrefix(line, "SELECT ")
}

func (Table) downScript(line string) bool {
	return strings.Contains(line, "DROP")
}

func (t Table) downReferenceScript(line string) bool {
	regex := regexp.MustCompile(`fkey|fk|foreign|foreign_key|foreignkey|foreignk|pkey|pk`)

	return regex.MatchString(line)
}

func (Table) downForeignkey(line string) bool {
	regex := regexp.MustCompile(`fkey|fk|foreign|foreign_key|foreignkey|foreignk`)

	return regex.MatchString(line)
}

func (Table) foreignScript(line string) bool {
	return strings.Contains(line, FOREIGN_KEY)
}

func (Table) refereceScript(line string, n int, lines []string) bool {
	return strings.Contains(line, ALTER_TABLE) && strings.Contains(lines[n+1], ADD_CONSTRAINT)
}
