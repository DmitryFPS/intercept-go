package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"intercept/internal/selfupdate"
)

var updatePublicKey = ""

func main() {
	if len(os.Args) < 2 || os.Args[1] != "apply" {
		os.Exit(2)
	}
	flags := flag.NewFlagSet("apply", flag.ContinueOnError)
	options := selfupdate.ApplyOptions{PublicKey: updatePublicKey}
	flags.StringVar(&options.BaseDir, "base-dir", "", "")
	flags.StringVar(&options.Package, "package", "", "")
	flags.StringVar(&options.Product, "product", "", "")
	flags.StringVar(&options.FromVersion, "from-version", "", "")
	flags.StringVar(&options.ToVersion, "to-version", "", "")
	flags.StringVar(&options.Application, "application", "", "")
	flags.StringVar(&options.Address, "address", "", "")
	flags.StringVar(&options.TokenFile, "token-file", "", "")
	flags.StringVar(&options.WorkRoot, "work-root", "", "")
	flags.StringVar(&options.ReadyFile, "ready-file", "", "")
	flags.IntVar(&options.OldPID, "old-pid", 0, "")
	if err := flags.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}
	if err := selfupdate.Apply(options); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
		os.Exit(1)
	}
}
