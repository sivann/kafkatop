package main

import (
	"fmt"
	"os"

	"github.com/sivann/kafkatop/cmd"
)

var (
	version = "1.16-go"
)

func main() {
	if err := cmd.Execute(version); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
