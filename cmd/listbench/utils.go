package listbench

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/hneemann/parser2/value"
	"go.mongodb.org/mongo-driver/mongo"
)

func downloadDataset(fileName string, downloadUrl string) string {
	filePath := path.Join(DataBaseDir, fileName)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		if _, err := os.Stat(DataBaseDir); os.IsNotExist(err) {
			os.Mkdir(DataBaseDir, 0755)
		}

		fmt.Println("Downloading", fileName, "from", downloadUrl)
		resp, err := http.Get(downloadUrl)
		if err != nil {
			log.Fatalln("Failed to download dataset:", err)
		}

		defer resp.Body.Close()

		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)

		out, err := os.Create(filePath)
		if err != nil {
			log.Fatalln("Failed to create file:", err)
		}

		defer out.Close()

		_, err = io.Copy(out, buf)
		if err != nil {
			log.Fatalln("Failed to write file:", err)
		}
	}

	return filePath
}

type benchmarkResults struct {
	queryId       int
	databaseName  string
	executionTime time.Duration
}

var queryIdCounter = make(map[string]int)
var benchResults = []benchmarkResults{}

func storeBenchmarkResults(databaseName string, executionTime time.Duration) {
	queryId := 0
	if _, ok := queryIdCounter[databaseName]; ok {
		queryIdCounter[databaseName]++
		queryId = queryIdCounter[databaseName]
	} else {
		queryIdCounter[databaseName] = 0
	}

	benchResults = append(benchResults, benchmarkResults{
		queryId:       queryId,
		databaseName:  databaseName,
		executionTime: executionTime,
	})
}

func exportBenchmarkResults(baseDir string) {
	if err := os.MkdirAll(baseDir, 0777); err != nil {
		log.Fatalln("Failed to create csv base directory at", baseDir, ":", err)
	}

	csvPath := filepath.Join(baseDir, fmt.Sprintf("benchmark-%v.csv", time.Now().Unix()))
	csvFile, err := os.Create(csvPath)
	if err != nil {
		log.Fatalln("Failed to create csv file at", csvPath, ":", err)
	}
	defer csvFile.Close()

	// TODO: maybe use encoding/csv for this
	_, err = csvFile.WriteString("queryId,databaseName,executionTime\n")
	if err != nil {
		log.Fatalln("Failed to write csv header:", err)
	}

	for _, result := range benchResults {
		_, err = csvFile.WriteString(fmt.Sprintf("%v,%v,%v\n", result.queryId, result.databaseName, float64(result.executionTime.Milliseconds())/1000.0))
		if err != nil {
			log.Fatalln("Failed to write csv line:", err)
		}
	}
}

func executeInMemoryQuery(parser *value.FunctionGenerator, operationName string, query string, dataName string, data value.Value) {
	fu, err := parser.Generate(query, dataName)
	if err != nil {
		log.Fatalln("Failed to generate function:", err)
	}

	fmt.Println("Executing", operationName+":", "\""+query+"\"")

	executionStartTime := time.Now()
	result, err := fu.Eval(data)
	if err != nil {
		log.Fatalln("Failed to call generated function:", err)
	}

	fmt.Println("Result:", result)
	fmt.Println("Execution time:", time.Since(executionStartTime))
	storeBenchmarkResults("parser2", time.Since(executionStartTime))
}

func executeSqlQuery(conn *sql.DB, dbName string, operationName string, query string) {
	fmt.Println("Executing", operationName+":", "\""+query+"\"")

	executionStartTime := time.Now()
	result, err := conn.Query(query)
	if err != nil {
		log.Fatalln("Failed to execute query:", err)
	}
	defer result.Close()

	var res float64
	result.Next()
	result.Scan(&res)

	fmt.Println("Result:", res)
	fmt.Println("Execution time:", time.Since(executionStartTime))
	storeBenchmarkResults(dbName, time.Since(executionStartTime))
}

func executeMongoQuery(operationName string, query func() (float64, error)) {
	fmt.Println("Executing", operationName)

	executionStartTime := time.Now()
	result, err := query()
	if err != nil {
		log.Fatalln("Failed to execute query:", err)
	}

	fmt.Println("Result:", result)
	fmt.Println("Execution time:", time.Since(executionStartTime))
	storeBenchmarkResults("mongodb", time.Since(executionStartTime))
}

func executeMongoCountQuery(operationName string, collection *mongo.Collection, filter interface{}) {
	executeMongoQuery(operationName, func() (float64, error) {
		count, err := collection.CountDocuments(context.Background(), filter)
		return float64(count), err
	})
}
