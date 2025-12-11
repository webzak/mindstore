package main

import (
	"fmt"
	"os"

	"github.com/webzak/mindstore/cmd/mindb/internal/collection"
	"github.com/webzak/mindstore/cmd/mindb/internal/server"
)

func main() {
	var err error
	if len(os.Args) < 2 {
		help()
		os.Exit(1)
	}
	command := os.Args[1]
	os.Args = os.Args[1:]
	switch command {
	case "collection", "c":
		err = collection.Run()
	case "server":
		err = server.Run()
	case "help", "-h", "--help":
		help()
		os.Exit(0)
	default:
		fmt.Printf("Unknown command: %s\n\n", command)
		help()
		os.Exit(1)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func help() {
	fmt.Println("mindb - Mindstore Database CLI Tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  mindb <command> [subcommand] [flags]")
	fmt.Println()
	fmt.Println("Available Commands:")
	fmt.Println("  collection    Manage collections (create, list, info, delete)")
	fmt.Println("  server        Start the mindb server")
	fmt.Println("  help          Show this help message")
	fmt.Println()
	fmt.Println("Use 'mindb <command> --help' for more information about a command.")
}
