package main

import (
	"database/sql"
	"log"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	_ "github.com/proullon/ramsql/driver"
)

type Movie struct {
	ID         	 int64 		`json:"id"`
	ImdbID       string 	`json:"imdbID"`
	Title        string 	`json:"title"`
	Year         int 			`json:"year"`
	Rating       float32  `json:"rating"`
	IsSuperhero  bool 		`json:"isSuperhero"`
}

func getAllMoviesHandler(c echo.Context) error{
	mvs := []Movie{}
	y := c.QueryParam("year")

	if y == "" {
		// query from db 
		rows , err := db.Query(`SELECT id, imdbID, title, year, rating, isSuperhero
		FROM goimdb`)
		if err != nil {
		 log.Fatal("Query error" , err)
		}
		defer rows.Close()

		for rows.Next() {
			var m Movie 
			if err := rows.Scan(&m.ID, &m.ImdbID, &m.Title, &m.Year , &m.Rating , &m.IsSuperhero) ; err != nil {
				return c.JSON(http.StatusInternalServerError , "scan"+err.Error())
			}
			mvs = append(mvs, m)
		}

		if err := rows.Err() ;err != nil {
			return c.JSON(http.StatusInternalServerError , "row"+err.Error())
		}
		
		return c.JSON(http.StatusOK , mvs)
	}

	year , err := strconv.Atoi(y)
	if err != nil {
		return c.JSON(http.StatusBadRequest , err.Error())
	}

	rows , err := db.Query(`SELECT id, imdbID, title, year, rating, isSuperhero
	FROM goimdb
	WHERE year = ? ` , year)
	if err != nil {
		return c.JSON(http.StatusInternalServerError ,err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var m Movie 
		if err := rows.Scan(&m.ID, &m.ImdbID, &m.Title, &m.Year , &m.Rating , &m.IsSuperhero) ; err != nil {
			return c.JSON(http.StatusInternalServerError , "scan"+err.Error())
		}
		mvs = append(mvs, m)
	}

	if err := rows.Err() ;err != nil {
		return c.JSON(http.StatusInternalServerError , "row"+err.Error())
	}

	return c.JSON(http.StatusOK ,mvs)
}

func getMovieByIdHandler(c echo.Context) error {
	imdbID := c.Param("imdbID")
	
	rows := db.QueryRow(`SELECT id, imdbID, title, year, rating, isSuperhero
	FROM goimdb WHERE imdbID=?` , imdbID)
	m := Movie{}
	err := rows.Scan(&m.ID, &m.ImdbID, &m.Title, &m.Year , &m.Rating , &m.IsSuperhero)
	switch err {
	case nil : 
		return c.JSON(http.StatusOK , m)
	case sql.ErrNoRows :
		return c.JSON(http.StatusNotFound , map[string]string{"message" : "movie not found"})
	default :
		return c.JSON(http.StatusInternalServerError , err.Error())
	}
}

func createMoviesHandler(c echo.Context) error  {
	m := &Movie{}
	
	if err := c.Bind(m) ; err != nil {
		return c.JSON(http.StatusBadRequest , err.Error()) 
	}

	// movies = append(movies, *m)
	stmt , err := db.Prepare(`
	INSERT INTO goimdb(imdbID , title , year , rating , isSuperhero) 
	VALUES (?,?,?,?,?)
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError , err.Error())
	}
	defer stmt.Close()

	// b := fmt.Sprintf("%v" , m.IsSuperhero)
	r , err := stmt.Exec(m.ImdbID , m.Title , m.Year , m.Rating , m.IsSuperhero)
	
	switch  {
	case err == nil :
		id , _ := r.LastInsertId()
		m.ID = id
		return c.JSON(http.StatusCreated , m)
	case err.Error() == "UNIQUE constranint violation" :
		return c.JSON(http.StatusConflict , "movie already exists")
	default :
		return c.JSON(http.StatusInternalServerError , err.Error())
	}
}

func updateMovieHandler(c echo.Context) error {
	// imdbID := c.Param("imdbID")

	m := &Movie{}
	if err := c.Bind(m); err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	stmt, err := db.Prepare(`
		UPDATE goimdb
		SET title=$1, year=$2, rating=$3, isSuperhero=$4
		WHERE imdbID=$5
	`)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(m.Title, m.Year, m.Rating, m.IsSuperhero , m.ImdbID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, m)
}

var db *sql.DB

func conn()  {
	var err error
	db , err = sql.Open("ramsql" , "goimdb")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	conn()

	createTb := `
	CREATE TABLE IF NOT EXISTS goimdb (
	id INT  AUTO_INCREMENT,
	imdbID TEXT NOT NULL UNIQUE ,
	title TEXT NOT NULL  ,
	year INT NOT NULL  ,
	rating FLOAT NOT NULL  ,
	isSuperhero BOOLEAN NOT NULL  ,
	PRIMARY KEY (id)
	);
	`
	if _ , err := db.Exec(createTb) ; err != nil {
		log.Fatal("Create table error:" , err)
		return 
	}

	e := echo.New()
	e.Use(middleware.Logger())

	e.GET("/movies" , getAllMoviesHandler)
	e.GET("/movies/:imdbID" , getMovieByIdHandler)

	e.POST("/movies"  , createMoviesHandler)

	e.PUT("movies/:imdbID" , updateMovieHandler)
	
	port := "2565"
	log.Println("starting... port", port)

	log.Fatal(e.Start(":" + port))
}