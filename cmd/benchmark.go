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
	"time"
)

const DataBaseDir = "data/"
const ImdbFilePath = "title.basics.tsv.gz"
const ImdbDownloadUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"

func downloadDataset(fileName string, downloadUrl string) string {
	filePath := path.Join(DataBaseDir, fileName)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
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
	Tconst         string
	TitleType      string
	PrimaryTitle   string
	OriginalTitle  string
	StartYear      uint16
	EndYear        uint16
	RuntimeMinutes uint16
	Genres         []string
}

func ImdbTitleFromCsvRecord(rec []string) ImdbTitle {
	return ImdbTitle{
		Tconst:        rec[0],
		TitleType:     rec[1],
		PrimaryTitle:  rec[2],
		OriginalTitle: rec[3],
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

func runImdbBenchmarks() {
	loadStartTime := time.Now()
	data, err := loadImdbData()
	if err != nil {
		log.Fatalln("Failed to load IMDB data:", err)
	}
	fmt.Println("Load time:", time.Since(loadStartTime))

	fmt.Println("Data length:", len(data))
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
