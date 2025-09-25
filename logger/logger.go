package logger

import (
	"fmt"
	"log"
	"os"

	"github.com/fatih/color"
)

func Info(v ...any) {
	m := fmt.Sprintln(v...)
	log.Printf("%s %s", color.GreenString("[info]"), m)
}

func Fail(v ...any) {
	m := fmt.Sprintln(v...)
	log.Printf("%s %s", color.RedString("[error]"), m)
}

func Failx(v ...any) {
	Fail(v...)
	os.Exit(1)
}
