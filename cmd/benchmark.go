package main

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/hneemann/parser2/value"
)

const DataBaseDir = "data/"
const ImdbFilePath = "title.basics.tsv.gz"
const ImdbDownloadUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"

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

type ImdbTitle struct {
	tconst         string
	titleType      string
	primaryTitle   string
	originalTitle  string
	isAdult        bool
	startYear      int32
	endYear        int32
	runtimeMinutes int32
	genres         []string
}

func ImdbTitleFromCsvRecord(rec []string) ImdbTitle {
	startYear, _ := strconv.Atoi(rec[5])
	endYear := startYear
	// \N = no value
	if rec[6] != "\\N" {
		endYear, _ = strconv.Atoi(rec[6])
	}

	runtimeMinutes, _ := strconv.Atoi(rec[7])

	// Rest strings are genres
	genres := []string{}
	for _, genreStr := range strings.Split(rec[8], ",") {
		genres = append(genres, genreStr)
	}

	return ImdbTitle{
		tconst:         rec[0],
		titleType:      rec[1],
		primaryTitle:   rec[2],
		originalTitle:  rec[3],
		isAdult:        rec[4] == "1",
		startYear:      int32(startYear),
		endYear:        int32(endYear),
		runtimeMinutes: int32(runtimeMinutes),
		genres:         genres,
	}
}

func loadImdbData() ([]ImdbTitle, error) {
	filePath := downloadDataset(ImdbFilePath, ImdbDownloadUrl)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileReader := io.Reader(file)
	gunzipReader, err := gzip.NewReader(fileReader)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(gunzipReader)
	csvReader.Comma = '\t' // CSV

	entries := []ImdbTitle{}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err == nil {
			entries = append(entries, ImdbTitleFromCsvRecord(record))
		}
	}

	return entries, nil
}

func executeQuery(parser *value.FunctionGenerator, operationName string, query string, dataName string, data value.Value) {
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

func runImdbBenchmarks() {
	loadStartTime := time.Now()
	data, err := loadImdbData()
	if err != nil {
		log.Fatalln("Failed to load IMDB data:", err)
	}

	fmt.Println("Load time:", time.Since(loadStartTime))
	fmt.Println("Data length:", len(data))

	// imdbTitleToMap := value.NewToMapReflection[ImdbTitle]()
	imdbTitleToMap := value.NewToMap[ImdbTitle]().
		Attr("tconst", func(t ImdbTitle) value.Value { return value.String(t.tconst) }).
		Attr("titleType", func(t ImdbTitle) value.Value { return value.String(t.titleType) }).
		Attr("primaryTitle", func(t ImdbTitle) value.Value { return value.String(t.primaryTitle) }).
		Attr("originalTitle", func(t ImdbTitle) value.Value { return value.String(t.originalTitle) }).
		Attr("isAdult", func(t ImdbTitle) value.Value { return value.Bool(t.isAdult) }).
		Attr("startYear", func(t ImdbTitle) value.Value { return value.Int(t.startYear) }).
		Attr("endYear", func(t ImdbTitle) value.Value { return value.Int(t.endYear) }).
		Attr("runtimeMinutes", func(t ImdbTitle) value.Value { return value.Int(t.runtimeMinutes) }).
		Attr("genres", func(t ImdbTitle) value.Value {
			return value.NewListConvert(func(s string) value.Value { return value.String(s) }, t.genres)
		})

	imdbTitles := value.NewListOfMaps[ImdbTitle](imdbTitleToMap, data)

	parser := value.New()

	executeQuery(parser, "count between 2000 and 2005", "imdb.filter(t -> t.startYear >= 2000 & t.startYear <= 2005).size()", "imdb", imdbTitles)
	executeQuery(parser, "average runtime", "imdb.map(t -> t.runtimeMinutes).average()", "imdb", imdbTitles)
	executeQuery(parser, "count containing \"You\" in primaryTitle", "imdb.filter(t -> t.primaryTitle.contains(\"You\")).size()", "imdb", imdbTitles)
	executeQuery(parser, "count of entries with three genres", "imdb.filter(t -> t.genres.size() = 3).size()", "imdb", imdbTitles)
	executeQuery(parser, "count of entries with genre Animation and Fantasy", "imdb.filter(t -> t.genres.contains(\"Animation\") & t.genres.contains(\"Fantasy\")).size()", "imdb", imdbTitles)
}

func main() {
	if len(os.Args) == 1 {
		fmt.Println("Usage: benchmark <bench>")
		fmt.Println("Available benchmarks: imdb")
		os.Exit(1)
	}

	bench := os.Args[1]
	switch bench {
	case "imdb":
		runImdbBenchmarks()
	default:
		fmt.Println("Unknown benchmark")
		os.Exit(1)
	}
}
