package listbench

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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const ImdbFileName = "title.basics.tsv.gz"
const ImdbDownloadUrl = "https://datasets.imdbws.com/title.basics.tsv.gz"

type ImdbTitle struct {
	TConst         string   `bson:"tconst"`
	TitleType      string   `bson:"titleType"`
	PrimaryTitle   string   `bson:"primaryTitle"`
	OriginalTitle  string   `bson:"originalTitle"`
	IsAdult        bool     `bson:"isAdult"`
	StartYear      int32    `bson:"startYear"`
	EndYear        int32    `bson:"endYear"`
	RuntimeMinutes int32    `bson:"runtimeMinutes"`
	Genres         []string `bson:"genres"`
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
		TConst:         rec[0],
		TitleType:      rec[1],
		PrimaryTitle:   rec[2],
		OriginalTitle:  rec[3],
		IsAdult:        rec[4] == "1",
		StartYear:      int32(startYear),
		EndYear:        int32(endYear),
		RuntimeMinutes: int32(runtimeMinutes),
		Genres:         genres,
	}
}

func loadImdbData() ([]ImdbTitle, error) {
	filePath := downloadDataset(ImdbFileName, ImdbDownloadUrl)
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
	defer gunzipReader.Close()

	csvReader := csv.NewReader(gunzipReader)
	csvReader.Comma = '\t' // CSV

	entries := []ImdbTitle{}
	isHeader := true

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if isHeader {
			// First row is header, skip it
			isHeader = false
			continue
		}

		if err == nil {
			entries = append(entries, ImdbTitleFromCsvRecord(record))
		}
	}

	return entries, nil
}

func importImdbIntoSqlDb(conn *sql.DB, imdbTitles []ImdbTitle) {
	// Check whether the table already exists and has the correct row count
	// If yes, skip the import
	// If not, drop the table and import the data
	r, err := conn.Query("SELECT COUNT(*) FROM imdb")
	if err == nil {
		defer r.Close()
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

		_, err = stmt.Exec(title.TConst, title.TitleType, title.PrimaryTitle, title.OriginalTitle, title.IsAdult, title.StartYear, title.EndYear, title.RuntimeMinutes, strings.Join(title.Genres, ","))
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

func importImdbIntoMongoDb(collection *mongo.Collection, imdbTitles []ImdbTitle) {
	// If count matches, assume data is correct and skip import
	count, err := collection.CountDocuments(context.Background(), bson.D{})
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

	// in-memory streaming query api
	imdbTitleToMap := value.NewToMap[ImdbTitle]().
		Attr("tconst", func(t ImdbTitle) value.Value { return value.String(t.TConst) }).
		Attr("titleType", func(t ImdbTitle) value.Value { return value.String(t.TitleType) }).
		Attr("primaryTitle", func(t ImdbTitle) value.Value { return value.String(t.PrimaryTitle) }).
		Attr("originalTitle", func(t ImdbTitle) value.Value { return value.String(t.OriginalTitle) }).
		Attr("isAdult", func(t ImdbTitle) value.Value { return value.Bool(t.IsAdult) }).
		Attr("startYear", func(t ImdbTitle) value.Value { return value.Int(t.StartYear) }).
		Attr("endYear", func(t ImdbTitle) value.Value { return value.Int(t.EndYear) }).
		Attr("runtimeMinutes", func(t ImdbTitle) value.Value { return value.Int(t.RuntimeMinutes) }).
		Attr("genres", func(t ImdbTitle) value.Value {
			return value.NewListConvert(func(s string) value.Value { return value.String(s) }, t.Genres)
		})

	imdbTitles := value.NewListOfMaps[ImdbTitle](imdbTitleToMap, data)
	parser := value.New()

	fmt.Println("Executing in-memory queries...")
	executeInMemoryQuery(parser, "count between 2000 and 2005", "imdb.filter(t -> t.startYear >= 2000 & t.startYear <= 2005).size()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "average runtime", "imdb.map(t -> t.runtimeMinutes).average()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "average runtime per titleType", "imdb.groupByString(t -> t.titleType).map(v -> {key: v.key, avgRuntime: v.values.map(t -> t.runtimeMinutes).average()}).eval()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "count containing \"You\" in primaryTitle", "imdb.filter(t -> t.primaryTitle.contains(\"You\")).size()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "count of entries with three genres", "imdb.filter(t -> t.genres.size() = 3).size()", "imdb", imdbTitles)
	executeInMemoryQuery(parser, "count of entries with genre Animation and Fantasy", "imdb.filter(t -> t.genres.contains(\"Animation\") & t.genres.contains(\"Fantasy\")).size()", "imdb", imdbTitles)

	// SQLite
	fmt.Println("Importing data into in-memory sqlite...")
	importStartTime := time.Now()
	importImdbIntoSqlDb(sqliteConn, data)
	fmt.Println("In-memory sqlite import done in", time.Since(importStartTime))

	fmt.Println("Executing SQL queries with in-memory sqlite...")
	executeSqlQuery(sqliteConn, "sqlite", "count between 2000 and 2005", "SELECT COUNT(*) FROM imdb WHERE startYear >= 2000 AND startYear <= 2005", "single")
	executeSqlQuery(sqliteConn, "sqlite", "average runtime", "SELECT AVG(runtimeMinutes) FROM imdb", "map")
	executeSqlQuery(sqliteConn, "sqlite", "average runtime per titleType", "SELECT titleType, AVG(runtimeMinutes) FROM imdb GROUP BY titleType", "single")
	executeSqlQuery(sqliteConn, "sqlite", "count containing \"You\" in primaryTitle", "SELECT COUNT(*) FROM imdb WHERE primaryTitle LIKE '%You%'", "single")
	executeSqlQuery(sqliteConn, "sqlite", "count of entries with three genres", "SELECT COUNT(*) FROM imdb WHERE LENGTH(genres) - LENGTH(REPLACE(genres, ',', '')) = 2", "single")
	executeSqlQuery(sqliteConn, "sqlite", "count of entries with genre Animation and Fantasy", "SELECT COUNT(*) FROM imdb WHERE genres LIKE '%Animation%' AND genres LIKE '%Fantasy%'", "single")

	// MariaDB
	fmt.Println("Importing data into MariaDB...")
	importStartTime = time.Now()
	importImdbIntoSqlDb(mariaDbConn, data)
	fmt.Println("MariaDB import done in", time.Since(importStartTime))

	fmt.Println("Executing SQL queries with MariaDB...")
	executeSqlQuery(mariaDbConn, "mariadb", "count between 2000 and 2005", "SELECT COUNT(*) FROM imdb WHERE startYear >= 2000 AND startYear <= 2005", "single")
	executeSqlQuery(mariaDbConn, "mariadb", "average runtime", "SELECT AVG(runtimeMinutes) FROM imdb", "map")
	executeSqlQuery(mariaDbConn, "mariadb", "average runtime per titleType", "SELECT titleType, AVG(runtimeMinutes) FROM imdb GROUP BY titleType", "single")
	executeSqlQuery(mariaDbConn, "mariadb", "count containing \"You\" in primaryTitle", "SELECT COUNT(*) FROM imdb WHERE primaryTitle LIKE '%You%'", "single")
	executeSqlQuery(mariaDbConn, "mariadb", "count of entries with three genres", "SELECT COUNT(*) FROM imdb WHERE LENGTH(genres) - LENGTH(REPLACE(genres, ',', '')) = 2", "single")
	executeSqlQuery(mariaDbConn, "mariadb", "count of entries with genre Animation and Fantasy", "SELECT COUNT(*) FROM imdb WHERE genres LIKE '%Animation%' AND genres LIKE '%Fantasy%'", "single")

	// MongoDB
	fmt.Println("Importing data into MongoDB...")
	importStartTime = time.Now()
	importImdbIntoMongoDb(mongoDbCollection, data)
	fmt.Println("MongoDB import done in", time.Since(importStartTime))

	fmt.Println("Executing MongoDB queries...")
	executeMongoCountQuery("count between 2000 and 2005", mongoDbCollection, bson.D{{"startYear", bson.D{{"$gte", 2000}, {"$lte", 2005}}}})
	executeMongoQuery("average runtime", func() (float64, error) {
		avg, err := mongoDbCollection.Aggregate(context.Background(), bson.A{
			bson.D{{"$group", bson.D{{"_id", nil}, {"avg", bson.D{{"$avg", "$runtimeMinutes"}}}}}},
		})
		if err != nil {
			return 0, err
		}
		defer avg.Close(context.Background())

		var result struct {
			Avg float64 `bson:"avg"`
		}
		avg.Next(context.Background())
		avg.Decode(&result)
		return result.Avg, nil
	})
	executeMongoQuery("average runtime per titleType", func() (float64, error) {
		avg, err := mongoDbCollection.Aggregate(context.Background(), bson.A{
			bson.D{{"$group", bson.D{{"_id", "$titleType"}, {"avg", bson.D{{"$avg", "$runtimeMinutes"}}}}}},
		})
		if err != nil {
			return 0, err
		}
		defer avg.Close(context.Background())

		count, averageRuntimeAcc := 0, 0.0
		var result struct {
			Id  string  `bson:"_id"`
			Avg float64 `bson:"avg"`
		}

		fmt.Println("Group results:")
		for avg.Next(context.Background()) {
			avg.Decode(&result)
			count++
			averageRuntimeAcc += result.Avg
			fmt.Printf("%s: %v\n", result.Id, result.Avg)
		}
		return averageRuntimeAcc / float64(count), nil
	})
	executeMongoCountQuery("count containing \"You\" in primaryTitle", mongoDbCollection, bson.D{{"primaryTitle", bson.D{{"$regex", "You"}}}})
	executeMongoCountQuery("count of entries with three genres", mongoDbCollection, bson.D{{"genres", bson.D{{"$size", 3}}}})
	executeMongoCountQuery("count of entries with genre Animation and Fantasy", mongoDbCollection, bson.D{{"genres", bson.D{{"$all", bson.A{"Animation", "Fantasy"}}}}})

	exportBenchmarkResults("imdb")
}
