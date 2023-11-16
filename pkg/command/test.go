package command

import (
	"fmt"
	"kmt/pkg/config"
	"os/exec"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
)

type test struct {
	config       config.Migration
	errorColor   *color.Color
	successColor *color.Color
}

func NewTest(config config.Migration, errorColor *color.Color, successColor *color.Color) test {
	return test{
		config:       config,
		errorColor:   errorColor,
		successColor: successColor,
	}
}

func (t test) Call() error {
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = " Test connections config..."
	progress.Start()

	for i, c := range t.config.Connections {
		progress.Stop()
		progress.Suffix = fmt.Sprintf(" Test connection to %s...", i)
		progress.Start()

		_, err := config.NewConnection(c)
		if err != nil {
			progress.Stop()
			progress.Suffix = fmt.Sprintf(" Unable to connect to %s...", t.errorColor.Sprint(i))
			progress.Start()

			t.errorColor.Println(err.Error())

			return nil
		}
	}

	progress.Stop()

	progress.Suffix = fmt.Sprintf(" Test '%s' command...", t.successColor.Sprint("pg_dump"))
	progress.Start()

	cli := exec.Command(t.config.PgDump, "--help")
	_, err := cli.CombinedOutput()
	if err != nil {
		progress.Stop()

		t.errorColor.Printf("'pg_dump' command not found on %s\n", t.config.PgDump)

		return nil
	}

	progress.Stop()

	t.successColor.Println("Config test passed")

	return nil
}
