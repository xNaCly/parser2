package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hneemann/parser2/cmd/benchmark"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Usage: benchmark <bench>")
		fmt.Println("Available benchmarks: imdb")
		os.Exit(1)
	}

	sqliteConn, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		log.Fatalln("Failed to open SQLite connection:", err)
	}
	defer sqliteConn.Close()

	// Connect to mariadb at localhost:3306 with user root and password "password"
	mariadbConn, err := sql.Open("mysql", "root:password@tcp(localhost:3306)/db")
	if err != nil {
		log.Fatalln("Failed to open MariaDB connection:", err)
	}
	defer mariadbConn.Close()

	bench := os.Args[1]
	switch bench {
	case "imdb":
		benchmark.RunImdbBenchmarks(sqliteConn, mariadbConn)
	default:
		fmt.Println("Unknown benchmark")
		os.Exit(1)
	}
}
