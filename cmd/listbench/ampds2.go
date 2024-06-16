package listbench

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hneemann/parser2/value"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const AMPds2RawFileName = "AMPds2.h5"
const AMPds2ConvertedFileName = "AMPds2.json.gz"

type AMPds2DataPoint struct {
	TimeStamp int64   `json:"timestamp"`
	Voltage   float64 `json:"voltage"`
	Current   float64 `json:"current"`
	Frequency float64 `json:"frequency"`
	Power     float64 `json:"power"`
}

func loadAMPds2Data() ([]AMPds2DataPoint, error) {
	ampRawFilePath := filepath.Join(DataBaseDir, AMPds2RawFileName)
	file, err := os.Open(ampRawFilePath)
	if err != nil {
		// URL changes every time and is only valid for a specified timespan.
		// Therefore we cannot automate this as we're unable to figure out the URL
		// without complicated stuff like parsing HTML.
		fmt.Println("Automatic download of AMPds2 data is not supported.")
		fmt.Println("Please download the .h5 file from the following link and place it in the data directory:")
		fmt.Println("https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/FIE0S4")
		fmt.Println("The file should be named ampds2.h5. After downloading run the benchmark again.")
		return nil, errors.New("Failed to open AMPds2 data file")
	}
	defer file.Close()

	// Check whether converted data file exists.
	// Ideally the HDF5 file would be read here directly and the relevant parts copied into memory.
	// However when using the most updated HDF5 library for Go https://github.com/gonum/hdf5
	// which is only a wrapper for the C library, it does not work.
	// The library can succesfully open the file, see the groups and the used dataset,
	// but when trying to read the dataset it fails by returning error -1 without any further information.
	// Instead we use a python script with the h5py library to convert the data to JSON
	// and then use that to import the data into memory.
	// For this comparison it does not make sense to spend more time on trying to use the HDF5 file directly
	// as it makes no difference for this comparison on how the data is loaded into RAM.
	// For an actual application it would obviously be better to use the HDF5 file directly
	// but for this comparison using an indirection via JSON is acceptable.
	ampds2ConvertedFilePath := filepath.Join(DataBaseDir, AMPds2ConvertedFileName)
	file, err = os.Open(ampds2ConvertedFilePath)
	if err != nil {
		fmt.Println("AMPds2 data needs conversion from HDF5 to JSON via a python script.")
		fmt.Println("Please run the following commands to create a venv, install the required packages, and convert the data:")
		fmt.Printf("python3 -m venv %s.venv\n", DataBaseDir)
		fmt.Printf("source %s.venv/bin/activate\n", DataBaseDir)
		fmt.Println("pip install -r cmd/listbench/ampds2_requirements.txt")
		fmt.Println("python cmd/listbench/ampds2_convert.py", ampRawFilePath)
		fmt.Println("After conversion, please run the benchmark again.")
		fmt.Printf("Please note that an installed hdf5 library is required for the conversion script (libhdf5-dev on ubuntu).\n\n")
		return nil, fmt.Errorf("AMPds2 data needs conversion from HDF5 to JSON")

	}
	defer file.Close()

	// Uncompress the JSON data using gzip
	fileReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}

	// Parse JSON data
	data := []AMPds2DataPoint{}
	return data, json.NewDecoder(fileReader).Decode(&data)
}

func importAMPds2IntoSqlDb(conn *sql.DB, ampds2Data []AMPds2DataPoint) {
	// Check whether the table already exists and has the correct row count
	// If yes, skip the import
	// If not, drop the table and import the data
	r, err := conn.Query("SELECT COUNT(*) FROM ampds2")
	if err == nil {
		defer r.Close()
		// Count worked, so the table exists
		var count int
		r.Next()
		r.Scan(&count)
		if count == len(ampds2Data) {
			fmt.Println("Table already imported and row count is correct. Assuming data is correct and skipping import.")
			return
		}
	}

	// Drop table if exists
	_, err = conn.Exec("DROP TABLE IF EXISTS ampds2")
	if err != nil {
		log.Fatalln("Failed to drop table:", err)
	}

	// Create table
	_, err = conn.Exec("CREATE TABLE ampds2 (timestamp BIGINT, voltage REAL, current REAL, frequency REAL, power REAL)")
	if err != nil {
		log.Fatalln("Failed to create table:", err)
	}

	// Import data using a prepared statement and a transaction
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		log.Fatalln("Failed to start transaction:", err)
	}

	stmt, err := tx.Prepare("INSERT INTO ampds2 (timestamp, voltage, current, frequency, power) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		log.Fatalln("Failed to prepare statement:", err)
	}

	for i, point := range ampds2Data {
		if i%1000 == 0 {
			fmt.Print("\rImporting ", i, " / ", len(ampds2Data))
		}

		_, err = stmt.Exec(point.TimeStamp, point.Voltage, point.Current, point.Frequency, point.Power)
		if err != nil {
			log.Fatalln("Failed to execute statement:", err)
		}
	}
	fmt.Println()

	err = tx.Commit()
	if err != nil {
		log.Fatalln("Failed to commit import transaction:", err)
	}
}

func importAMPds2IntoMongoDb(collection *mongo.Collection, ampds2Data []AMPds2DataPoint) {
	// Check whether the collection already exists and has the correct row count
	// If yes, skip the import
	// If not, drop the collection and import the data
	count, err := collection.CountDocuments(context.Background(), nil)
	if err == nil {
		// Count worked, so the collection exists
		if count == int64(len(ampds2Data)) {
			fmt.Println("Collection already imported and row count is correct. Assuming data is correct and skipping import.")
			return
		}
	}

	// Drop collection if exists
	err = collection.Drop(context.Background())
	if err != nil {
		log.Fatalln("Failed to drop collection:", err)
	}

	// We need to convert the data to a slice of interfaces because
	// of the way the MongoDB driver works. It only accepts interface slices.
	interfaceData := make([]interface{}, 0, len(ampds2Data))
	for _, entry := range ampds2Data {
		interfaceData = append(interfaceData, entry)
	}

	// Import data
	_, err = collection.InsertMany(context.Background(), interfaceData)
	if err != nil {
		log.Fatalln("Failed to insert documents:", err)
	}
}

func RunAMPds2Benchmarks(sqliteConn, mariadbConn *sql.DB, mongoDbCollection *mongo.Collection) {
	fmt.Println("Loading AMPds2 data...")
	loadStartTime := time.Now()
	data, err := loadAMPds2Data()
	if err != nil {
		log.Fatalln("Failed to load IMDB data:", err)
	}

	fmt.Println("Load time:", time.Since(loadStartTime))
	fmt.Println("Data length:", len(data))
	fmt.Printf("%+v\n", data[0:5])

	// in-memory streaming query api
	ampds2ToMap := value.NewToMap[AMPds2DataPoint]().
		Attr("timestamp", func(t AMPds2DataPoint) value.Value { return value.Int(t.TimeStamp) }).
		Attr("voltage", func(t AMPds2DataPoint) value.Value { return value.Float(t.Voltage) }).
		Attr("current", func(t AMPds2DataPoint) value.Value { return value.Float(t.Current) }).
		Attr("frequency", func(t AMPds2DataPoint) value.Value { return value.Float(t.Frequency) }).
		Attr("power", func(t AMPds2DataPoint) value.Value { return value.Float(t.Power) })

	ampds2Maps := value.NewListOfMaps[AMPds2DataPoint](ampds2ToMap, data)
	parser := value.New()

	fmt.Println("Executing in-memory queries...")
	executeInMemoryQuery(parser, "average power usage per day", `ampds2.map(d -> d.power).average()`, "ampds2", ampds2Maps)
	executeInMemoryQuery(parser, "average power usage per day", `ampds2.groupByInt(d -> d.timestamp / 86400 / 1e9).map(g -> g.values.map(d -> d.power).average())`, "ampds2", ampds2Maps)
	executeInMemoryQuery(parser, "voltage difference per day sorted descending", `ampds2.groupByInt(d -> d.timestamp / 86400 / 1e9).map(g -> let m = g.values.minMax(d -> d.voltage); m.max - m.min).orderRev(d -> d)`, "ampds2", ampds2Maps)
	executeInMemoryQuery(parser, "count of days with momentary frequency derivation exceeding one hertz", `ampds2.groupByInt(d -> d.timestamp / 86400 / 1e9).filter(g -> let m = g.values.minMax(d -> d.frequency); (m.max - m.min > 1.0)).size()`, "ampds2", ampds2Maps)
	executeInMemoryQuery(parser, "minutes with no power", `ampds2.filter(d -> d.power = 0.0).size()`, "ampds2", ampds2Maps)

	// SQLite
	fmt.Println("Importing AMPds2 data into in-memory sqlite...")
	importStartTime := time.Now()
	importAMPds2IntoSqlDb(sqliteConn, data)
	fmt.Println("In-memory sqlite import done in", time.Since(importStartTime))

	fmt.Println("Executing SQL queries with in-memory sqlite...")
	executeSqlQuery(sqliteConn, "sqlite", "average power usage per day", "SELECT AVG(power) FROM ampds2", "single")
	executeSqlQuery(sqliteConn, "sqlite", "average power usage per day", "SELECT AVG(power) FROM ampds2 GROUP BY CAST(timestamp / 86400 / 1e9 as INT)", "list")
	executeSqlQuery(sqliteConn, "sqlite", "voltage difference per day sorted descending", "SELECT MAX(voltage) - MIN(voltage) as diff FROM ampds2 GROUP BY CAST(timestamp / 86400 / 1e9 as INT) ORDER BY diff DESC", "list")
	executeSqlQuery(sqliteConn, "sqlite", "count of days with momentary frequency derivation exceeding one hertz", "SELECT COUNT(*) FROM (SELECT COUNT(*) FROM ampds2 GROUP BY CAST(timestamp / 86400 / 1e9 as INT) HAVING MAX(frequency) - MIN(frequency) > 1.0) as subquery", "single")
	executeSqlQuery(sqliteConn, "sqlite", "minutes with no power", "SELECT COUNT(*) FROM ampds2 WHERE power = 0.0", "single")

	// MariaDB
	fmt.Println("Importing AMPds2 data into MariaDB...")
	importStartTime = time.Now()
	importAMPds2IntoSqlDb(mariadbConn, data)
	fmt.Println("MariaDB import done in", time.Since(importStartTime))

	fmt.Println("Executing SQL queries with MariaDB...")
	executeSqlQuery(mariadbConn, "mariadb", "average power usage per day", "SELECT AVG(power) FROM ampds2", "single")
	executeSqlQuery(mariadbConn, "mariadb", "average power usage per day", "SELECT AVG(power) FROM ampds2 GROUP BY CAST(timestamp / 86400 / 1e9 as INT)", "list")
	executeSqlQuery(mariadbConn, "mariadb", "voltage difference per day sorted descending", "SELECT MAX(voltage) - MIN(voltage) as diff FROM ampds2 GROUP BY CAST(timestamp / 86400 / 1e9 as INT) ORDER BY diff DESC", "list")
	executeSqlQuery(mariadbConn, "mariadb", "count of days with momentary frequency derivation exceeding one hertz", "SELECT COUNT(*) FROM (SELECT COUNT(*) FROM ampds2 GROUP BY CAST(timestamp / 86400 / 1e9 as INT) HAVING MAX(frequency) - MIN(frequency) > 1.0) as subquery", "single")
	executeSqlQuery(mariadbConn, "mariadb", "minutes with no power", "SELECT COUNT(*) FROM ampds2 WHERE power = 0.0", "single")

	// MongoDB
	fmt.Println("Importing AMPds2 data into MongoDB...")
	importStartTime = time.Now()
	importAMPds2IntoMongoDb(mongoDbCollection, data)
	fmt.Println("MongoDB import done in", time.Since(importStartTime))

	fmt.Println("Executing MongoDB queries...")

	executeMongoQuery("average power usage per day", func() (float64, error) {
		avg, err := mongoDbCollection.Aggregate(context.Background(), bson.A{
			bson.D{{"$group", bson.D{{"_id", nil}, {"avg", bson.D{{"$avg", "$power"}}}}}},
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
	executeMongoQuery("average power usage per day", func() ([]float64, error) {
		avg, err := mongoDbCollection.Aggregate(context.Background(), bson.A{
			bson.D{
				{"$group", bson.D{
					{"_id", bson.D{{"$toInt", bson.D{{"$divide", bson.A{"$timestamp", 86400 * 1e9}}}}}},
					{"avg", bson.D{{"$avg", "$power"}}},
				}},
			},
		})

		if err != nil {
			return nil, err
		}
		defer avg.Close(context.Background())

		var result []float64
		for avg.Next(context.Background()) {
			var entry struct {
				Id  int     `bson:"_id"`
				Avg float64 `bson:"avg"`
			}
			avg.Decode(&entry)
			result = append(result, entry.Avg)
		}
		return result, nil
	})
	executeMongoQuery("voltage difference per day sorted descending", func() ([]float64, error) {
		diffs, err := mongoDbCollection.Aggregate(context.Background(), bson.A{
			bson.D{
				{"$group", bson.D{
					{"_id", bson.D{{"$toInt", bson.D{{"$divide", bson.A{"$timestamp", 86400 * 1e9}}}}}},
					{"max", bson.D{{"$max", "$voltage"}}},
					{"min", bson.D{{"$min", "$voltage"}}},
				}},
			},
			bson.D{
				{"$project", bson.D{
					{"_id", 0},
					{"diff", bson.D{{"$subtract", bson.A{"$max", "$min"}}}},
				}},
			},
			bson.D{
				{"$sort", bson.D{{"diff", -1}}},
			},
		})

		if err != nil {
			return nil, err
		}
		defer diffs.Close(context.Background())

		var result []float64
		for diffs.Next(context.Background()) {
			var entry struct {
				Diff float64 `bson:"diff"`
			}
			diffs.Decode(&entry)
			result = append(result, entry.Diff)
		}
		return result, nil
	})
	executeMongoQuery("count of days with momentary frequency derivation exceeding one hertz", func() (float64, error) {
		count, err := mongoDbCollection.Aggregate(context.Background(), bson.A{
			bson.D{
				{"$group", bson.D{
					{"_id", bson.D{{"$toInt", bson.D{{"$divide", bson.A{"$timestamp", 86400 * 1e9}}}}}},
					{"max", bson.D{{"$max", "$frequency"}}},
					{"min", bson.D{{"$min", "$frequency"}}},
				}},
			},
			bson.D{
				{"$match", bson.D{
					{"$expr", bson.D{
						{"$gt", bson.A{bson.D{{"$subtract", bson.A{"$max", "$min"}}}, 1.0}},
					}},
				}},
			},
			bson.D{
				{"$count", "count"},
			},
		})

		if err != nil {
			return 0, err
		}
		defer count.Close(context.Background())

		var result struct {
			Count float64 `bson:"count"`
		}
		count.Next(context.Background())
		count.Decode(&result)
		return result.Count, nil
	})
	executeMongoCountQuery("minutes with no power", mongoDbCollection, bson.D{{"power", 0.0}})

	exportBenchmarkResults("ampds2")
}
