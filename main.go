package main

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	//"github.com/jinzhu/gorm"
	//_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	//"log"

	"gopkg.in/matryer/respond.v1"

	"net/http"
	"regexp"
)

// urls -> URL database structure
type urls struct {
	ID 		uint64	`json:"id" db:"id"`
	Tinyurl string `json:"tiny_url" db:"tiny_url"`
	Longurl string	`json:"long_url" db:"long_url""`
}

// PostgresClient -> Provides a connection to the postgres database server
/*func PostgresClient() *gorm.DB {
	dbClient, err := gorm.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=mydb password=postgres sslmode=disable")
	if err != nil {
		panic(err)
	}
	return dbClient
}*/

func PostgresClient() *sqlx.DB {
	db, err := sqlx.Connect("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=mydb password=postgres sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}

// RedisClient -> Provides a connection to the Redis server
/*func RedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return client
}*/


// StoreTinyURL -> puts the urls into cache and DB
func StoreTinyURL(dbURLData urls, longURL string, tinyURL string, db *sqlx.DB) {
	_, err := db.NamedExec(`INSERT INTO urls (tiny_url,long_url) VALUES (:tinyurl,:longurl)`, map[string]interface{}{
			"tinyurl": dbURLData.Tinyurl,
			"longurl": dbURLData.Longurl,
		})
	if err != nil {
		panic(err)
	}
	//dbClient.Create(&dbURLData)
	//redisClient.HSet("urls", tinyURL, longURL)
}

// GenerateHashAndInsert -> Genarates a unique tiny URL and inserts it to DB
func GenerateHashAndInsert(longURL string, startIndex int, db *sqlx.DB ) (string,error) {
	byteURLData := []byte(longURL)
	hashedURLData := fmt.Sprintf("%x", md5.Sum(byteURLData))
	tinyURLRegex, err := regexp.Compile("[/+]")
	if err != nil {
		return "Unable to generate tiny URL", err
	}
	tinyURLData := tinyURLRegex.ReplaceAllString(base64.URLEncoding.EncodeToString([]byte(hashedURLData)), "_")
	if len(tinyURLData) < (startIndex + 6) {
		return "Unable to generate tiny URL" , err
	}
	tinyURL := tinyURLData[startIndex : startIndex+6]
	var dbURLData urls
	q := `select * from urls where tiny_url = $1`
	err = db.Get(&dbURLData, q, tinyURL)
	if err != nil{
		return "error getting data from db", err
	}
	if dbURLData.Tinyurl == "" {
		fmt.Println(dbURLData, "in not found")
		go StoreTinyURL(urls{Tinyurl: tinyURL, Longurl: longURL}, longURL, tinyURL, db)
		return tinyURL ,nil
	} else if (dbURLData.Tinyurl == tinyURL) && (dbURLData.Longurl == longURL) {
		fmt.Println(dbURLData, "in found and equal")
		return tinyURL ,nil
	} else {
		return GenerateHashAndInsert(longURL, startIndex+1, db)
	}
}


func GetTinyHandler(res http.ResponseWriter, req *http.Request, db *sqlx.DB) {
	requestParams, err := req.URL.Query()["longUrl"]
	if !err || len(requestParams[0]) < 1 {
		err := map[string]string{"Long_URL":"URL parameter longUrl is missing"}
		respond.With(res, req, http.StatusOK, err)

	} else {
		longURL := requestParams[0]
		tinyURL, err := GenerateHashAndInsert(longURL, 0, db)
		if err != nil{
			err := map[string]error{"error":err}
			respond.With(res, req, http.StatusBadRequest, err)
		}else {
			data := map[string]string{"Tiny_URL":tinyURL}
			respond.With(res, req, http.StatusOK, data)
			//redisClient.HSet("urls", tinyURL, longURL)
		}

	}
}

func GetLongHandler(res http.ResponseWriter, req *http.Request, db *sqlx.DB) {
	requestParams, err := req.URL.Query()["tinyUrl"]
	if !err || len(requestParams[0]) < 1 {
		err := map[string]string{"Long_URL":"URL parameter tinyUrl is missing"}
		respond.With(res, req, http.StatusOK, err)
	}
	tinyURL := requestParams[0]

	var url urls
	q := `select * from urls where tiny_url = $1`
	er := db.Get(&url, q, tinyURL)
	if er != nil{
		err := map[string]error{"error": er}
		respond.With(res, req, http.StatusBadRequest, err)
	}
	if url.Longurl != "" {
		//redisClient.HSet("urls", tinyURL, url.Longurl)
		data := map[string]string{"Long_URL":url.Longurl}
		respond.With(res, req, http.StatusOK, data)
	} else {
		err := map[string]string{"error":"Unable to find long URL"}
		respond.With(res, req, http.StatusBadRequest, err)
	}
}



func main() {
	/*redisClient := RedisClient()

	pong, err := redisClient.Ping().Result()
	fmt.Println("Redis ping", pong, err)*/

	db := PostgresClient()
	defer db.Close()
	serverInstance := &http.Server{
		Addr: ":8080",
	}

	http.HandleFunc("/long/", func(w http.ResponseWriter, r *http.Request) {
		GetLongHandler(w, r, db)
		})

	http.HandleFunc("/tiny/", func(w http.ResponseWriter, r *http.Request) {
		GetTinyHandler(w, r, db)
		})

	serverInstance.ListenAndServe()

}
