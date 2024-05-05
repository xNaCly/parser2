package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hneemann/parser2/cmd/listbench"
	_ "github.com/mattn/go-sqlite3"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Usage: benchmark <bench>")
		fmt.Println("Available benchmarks: imdb")
		os.Exit(1)
	}

	fmt.Println("Setting up database connections...")

	// SQLite (in-memory)
	sqliteConn, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		log.Fatalln("Failed to open SQLite connection:", err)
	}
	defer sqliteConn.Close()

	// MariaDB
	mariadbConn, err := sql.Open("mysql", "root:password@tcp(localhost:3306)/db")
	if err != nil {
		log.Fatalln("Failed to open MariaDB connection:", err)
	}
	defer mariadbConn.Close()

	// MongoDB
	mongoDbConnectionString := "mongodb://root:password@localhost:27017"
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(mongoDbConnectionString).SetServerAPIOptions(serverAPI)
	mongoDbConn, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		log.Fatalln("Failed to open MongoDB connection:", err)
	}
	defer mongoDbConn.Disconnect(context.Background())
	mongoDbCollection := mongoDbConn.Database("db").Collection("imdb")

	fmt.Println("Successfully setup databases connections")

	bench := os.Args[1]
	switch bench {
	case "imdb":
		listbench.RunImdbBenchmarks(sqliteConn, mariadbConn, mongoDbCollection)
	default:
		fmt.Println("Unknown benchmark")
		os.Exit(1)
	}
}
