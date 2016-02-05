package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"
	"github.com/stripe/stripe-go"
)

const (
	PageSize           = 100
	ReportingIncrement = 100
)

type Charge struct {
	ID      string    `gorm:"column:id;primary_key"`
	Amount  uint64    `gorm:"column:amount"`
	Created time.Time `gorm:"column:created"`
	Offset  uint64    `gorm:"column:event_offset"`
}

type Conf struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
}

// Use a custom event implementation because the one included with the stripe
// package doesn't have our special "offset" field.
type Event struct {
	Data   stripe.EventData `json:"data"`
	Offset uint64           `json:"offset"`
	Type   string           `json:"type"`
}

type Page struct {
	Data    []Event `json:"data"`
	HasMore bool    `json:"has_more"`
	Object  string  `json:"object"`
	URL     string  `json:"url"`
}

func main() {
	var conf Conf
	err := envdecode.Decode(&conf)
	if err != nil {
		log.Fatal(err)
	}

	dbSQL, err := sql.Open("postgres", conf.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}

	db, err := gorm.Open("postgres", dbSQL)
	if err != nil {
		log.Fatal(err)
	}

	err = consumeEvents(db)
	if err != nil {
		log.Fatal(err)
	}
}

func consumeEvents(db gorm.DB) error {
	var offset uint64
	client := &http.Client{}
	numProcessed := 0
	start := time.Now()

	for {
		startPage := time.Now()

		url := "http://localhost:8080/events"
		if offset != 0 {
			url = fmt.Sprintf("%s?starting_after=%v", url, offset)
		}
		log.Printf("Requesting page: %v", url)

		resp, err := client.Get(url)
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			return fmt.Errorf("Non-200 response from server")
		}

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var page Page
		err = json.Unmarshal(data, &page)
		if err != nil {
			return err
		}

		tx := db.Begin()

		for _, event := range page.Data {
			switch event.Type {
			case "charge.created":
				//log.Printf("amount = %v", event.Data.Obj["amount"])
				charge := Charge{
					// TODO: deserialize to proper charge object
					ID:      event.Data.Obj["id"].(string),
					Amount:  uint64(event.Data.Obj["amount"].(float64)),
					Created: time.Unix(int64(event.Data.Obj["created"].(float64)), 0),
					// TODO: should actually be in its own table
					Offset: event.Offset,
				}
				//tx.FirstOrCreate(&charge)
				tx.Create(&charge)

				// TODO: this doesn't seem to do anything even on error
				if tx.Error != nil {
					return tx.Error
				}

				// Set offset for the next page request.
				offset = event.Offset
			}
		}

		tx.Commit()

		log.Printf("Processed page of %v event(s) in %v.",
			len(page.Data), time.Now().Sub(startPage))

		numProcessed += len(page.Data)
		if !page.HasMore {
			log.Printf("Reached end of the log. Processed %v event(s) in %v.",
				numProcessed, time.Now().Sub(start))
		}
	}

	return nil
}
