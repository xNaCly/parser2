package listbench

import (
	"database/sql"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

func RunNationsDbBenchmarks(sqliteConn *sql.DB, mariadbConn *sql.DB, mongoDbCollection *mongo.Collection) {
	fmt.Println("Loading nationsdb data...")

}
