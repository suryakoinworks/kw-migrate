package db

import (
	"fmt"
	"kmt/pkg/config"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type (
	table struct {
		command string
		config  config.Connection
	}

	Ddl struct {
		Name       string
		Definition migration
		Reference  migration
		ForeignKey migration
	}
)

func NewTable(command string, config config.Connection) table {
	return table{command: command, config: config}
}

func (t table) Generate(name string, schemaOnly bool) Ddl {
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
		"--table", name,
		t.config.Name,
	}

	if schemaOnly {
		options = append(options, "--schema-only")
	} else {
		options = append(options, "--inserts")
	}

	cli := exec.Command(t.command, options...)
	cli.Env = os.Environ()
	cli.Env = append(cli.Env, fmt.Sprintf("PGPASSWORD=%s", t.config.Password))

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
		if t.skip(line) || skip {
			skip = false

			continue
		}

		if t.downScript(line) {
			if t.downReferenceScript(line) {
				if t.downForeignkey(line) {
					downForeignScript = append(downForeignScript, line)
				} else {
					downReferenceScript = append(downReferenceScript, line)
				}
			} else {
				downScript = append(downScript, line)
			}
		} else {
			if t.refereceScript(line, n, lines) {
				if t.foreignScript(lines[n+1]) {
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

	return Ddl{
		Name: name,
		Definition: migration{
			UpScript:   strings.Replace(strings.Replace(strings.Join(upScript, "\n"), "CREATE TABLE", "CREATE TABLE IF EXISTS", -1), "CREATE SEQUENCE", "CREATE SEQUENCE IF EXISTS", -1),
			DownScript: strings.Join(downScript, "\n"),
		},
		Reference: migration{
			UpScript:   strings.Join(upReferenceScript, "\n"),
			DownScript: strings.Join(downReferenceScript, "\n"),
		},
		ForeignKey: migration{
			UpScript:   strings.Join(upForeignScript, "\n"),
			DownScript: strings.Join(downForeignScript, "\n"),
		},
	}
}

func (table) skip(line string) bool {
	return line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET ") || strings.HasPrefix(line, "SELECT ")
}

func (table) downScript(line string) bool {
	return strings.Contains(line, "DROP")
}

func (table) downReferenceScript(line string) bool {
	return strings.Contains(line, "pkey") || strings.Contains(line, "fkey") || strings.Contains(line, "pk") || strings.Contains(line, "fk")
}

func (table) downForeignkey(line string) bool {
	return strings.Contains(line, "fkey") || strings.Contains(line, "fk")
}

func (table) foreignScript(line string) bool {
	return strings.Contains(line, "FOREIGN KEY")
}

func (table) refereceScript(line string, n int, lines []string) bool {
	return strings.Contains(line, "ALTER TABLE ONLY") && strings.Contains(lines[n+1], "ADD CONSTRAINT")
}
