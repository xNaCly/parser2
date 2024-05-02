package benchmark

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hneemann/parser2/value"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const DataBaseDir = "data/"
const ImdbFilePath = "title.basics.tsv.gz"
const ImdbDownloadUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"

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

func importImdbIntoSqlDB(conn *sql.DB, imdbTitles []ImdbTitle) {
	// Check whether the table already exists and has the correct row count
	// If yes, skip the import
	// If not, drop the table and import the data
	r, err := conn.Query("SELECT COUNT(*) FROM imdb")
	if err == nil {
		// Count worked, so the table exists
		var count int
		r.Next()
		r.Scan(&count)
		if count == len(imdbTitles) {
			fmt.Println("Table already imported and row count is correct. Assuming data is correct and skipping import.")
			return
		}
	}

	// Drop table if exists
	_, err = conn.Exec("DROP TABLE IF EXISTS imdb")
	if err != nil {
		log.Fatalln("Failed to drop table:", err)
	}

	// Create table
	_, err = conn.Exec("CREATE TABLE imdb (tconst TEXT, titleType TEXT, primaryTitle TEXT, originalTitle TEXT, isAdult INTEGER, startYear INTEGER, endYear INTEGER, runtimeMinutes INTEGER, genres TEXT)")
	if err != nil {
		log.Fatalln("Failed to create table:", err)
	}

	// Import data using a prepared statement and a transaction
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		log.Fatalln("Failed to start transaction:", err)
	}

	stmt, err := tx.Prepare("INSERT INTO imdb (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatalln("Failed to prepare statement:", err)
	}

	for i, title := range imdbTitles {
		if i%1000 == 0 {
			fmt.Print("\rImporting ", i, " / ", len(imdbTitles))
		}

		_, err = stmt.Exec(title.tconst, title.titleType, title.primaryTitle, title.originalTitle, title.isAdult, title.startYear, title.endYear, title.runtimeMinutes, strings.Join(title.genres, ","))
		if err != nil {
			log.Fatalln("Failed to insert data into sql database:", err)
		}
	}
	fmt.Println()

	err = tx.Commit()
	if err != nil {
		log.Fatalln("Failed to commit import transaction:", err)
	}
}

func importImdbIntoMongoDB(collection *mongo.Collection, imdbTitles []ImdbTitle) {
	// If count matches, assume data is correct and skip import
	count, err := collection.CountDocuments(context.Background(), nil)
	if err == nil && count == int64(len(imdbTitles)) {
		fmt.Println("Collection already imported and row count is correct. Assuming data is correct and skipping import.")
		return
	}

	// Drop collection if exists
	collection.Drop(context.Background())

	// We need to convert the data to a slice of interfaces because
	// of the way the MongoDB driver works. It only accepts interface slices.
	interfaceData := make([]interface{}, 0, len(imdbTitles))
	for _, entry := range imdbTitles {
		interfaceData = append(interfaceData, entry)
	}

	_, err = collection.InsertMany(context.Background(), interfaceData)
	if err != nil {
		log.Fatalln("Failed to insert data into MongoDB:", err)
	}
}

func RunImdbBenchmarks(sqliteConn *sql.DB, mariaDbConn *sql.DB, mongoDbCollection *mongo.Collection) {
	fmt.Println("Loading IMDB data...")
	loadStartTime := time.Now()
	data, err := loadImdbData()
	if err != nil {
		log.Fatalln("Failed to load IMDB data:", err)
	}

	fmt.Println("Load time:", time.Since(loadStartTime))
	fmt.Println("Data length:", len(data))

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

	executeInMemoryQuery(parser, "count between 2000 and 2005", "imdb.filter(t -> t.startYear >= 2000 & t.startYear <= 2005).size()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "average runtime", "imdb.map(t -> t.runtimeMinutes).average()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "count containing \"You\" in primaryTitle", "imdb.filter(t -> t.primaryTitle.contains(\"You\")).size()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "count of entries with three genres", "imdb.filter(t -> t.genres.size() = 3).size()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "count of entries with genre Animation and Fantasy", "imdb.filter(t -> t.genres.contains(\"Animation\") & t.genres.contains(\"Fantasy\")).size()", "imdb", imdbTitles)

	// Import data into SQLite
	fmt.Println("Importing data into in-memory sqlite...")
	importStartTime := time.Now()
	importImdbIntoSqlDB(sqliteConn, data)
	fmt.Println("In-memory sqlite import done in", time.Since(importStartTime))

	// Count entries in SQLite table
	r, err := sqliteConn.Query("SELECT COUNT(*) FROM imdb")
	if err != nil {
		log.Fatalln("Failed to count entries in sqlite database:", err)
	}

	var count int
	r.Next()
	r.Scan(&count)
	fmt.Println("Count of entries in sqlite database:", count)

	// Same for mariadb
	fmt.Println("Importing data into mariadb...")
	importStartTime = time.Now()
	importImdbIntoSqlDB(mariaDbConn, data)
	fmt.Println("MariaDB import done in", time.Since(importStartTime))

	r, err = mariaDbConn.Query("SELECT COUNT(*) FROM imdb")
	if err != nil {
		log.Fatalln("Failed to count entries in mariadb database:", err)
	}

	r.Next()
	r.Scan(&count)
	fmt.Println("Count of entries in mariadb database:", count)

	// Import data into MongoDB
	fmt.Println("Importing data into MongoDB...")
	importStartTime = time.Now()
	mongoDbCollection.Drop(context.Background())

	interfaceData := make([]interface{}, 0, len(data))
	for _, entry := range data {
		interfaceData = append(interfaceData, entry)
	}

	ordered := false
	_, err = mongoDbCollection.InsertMany(context.Background(), interfaceData, &options.InsertManyOptions{
		Ordered: &ordered,
	})
	if err != nil {
		log.Fatalln("Failed to insert data into MongoDB:", err)
	}
	fmt.Println("MongoDB import done in", time.Since(importStartTime))
}
