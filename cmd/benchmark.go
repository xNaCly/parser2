package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

const ImdbFilePath = "title.basics.tsv.gz"

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
	file, err := os.Open(ImdbFilePath)
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
