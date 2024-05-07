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
}

func executeSqlQuery(conn *sql.DB, operationName string, query string) {
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
}

func executeMongoCountQuery(operationName string, collection *mongo.Collection, filter interface{}) {
	executeMongoQuery(operationName, func() (float64, error) {
		count, err := collection.CountDocuments(context.Background(), filter)
		return float64(count), err
	})
}
