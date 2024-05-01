package benchmark

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/hneemann/parser2/value"
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
