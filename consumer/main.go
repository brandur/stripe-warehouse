package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/joeshaw/envdecode"
	"github.com/lib/pq"
	"github.com/stripe/stripe-go"
)

const (
	PageBuffer         = 10
	ReportingIncrement = 100
)

type Conf struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
	StripeKey   string `env:"STRIPE_KEY,required"`
	StripeURL   string `env:"STRIPE_URL,default=https://api.stripe.com"`
}

// Use a custom event implementation because the one included with the stripe
// package doesn't have our special "offset" field.
type Event struct {
	Data     stripe.EventData `json:"data"`
	Sequence uint64           `json:"sequence"`
	Type     string           `json:"type"`
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

	db, err := sql.Open("postgres", conf.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}

	doneChan := make(chan int)
	pageChan := make(chan Page, PageBuffer)
	start := time.Now()

	// Request events from the API.
	go func() {
		err := requestEvents(conf.StripeKey, conf.StripeURL, doneChan, pageChan)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// And simultaneously, load them to Postgres.
	numProcessed, err := loadEvents(doneChan, pageChan, db)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Reached end of the log. Processed %v event(s) in %v.",
		numProcessed, time.Now().Sub(start))
}

func loadEvents(doneChan chan int, pageChan chan Page, db *sql.DB) (int, error) {
	for {
		select {
		case page := <-pageChan:
			err := loadEventsPage(page, db)
			if err != nil {
				return 0, err
			}

		default:
			select {
			case numProcessed := <-doneChan:
				return numProcessed, nil
			default:
				// If we didn't get a done signal, sleep for a short time just
				// to keep this goroutine from spinning like crazy before the
				// first batch of work comes in.
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func loadEventsPage(page Page, db *sql.DB) error {
	startPage := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	statement, err := tx.Prepare(pq.CopyIn("charges",
		"id", "amount", "created", "sequence"))
	if err != nil {
		return err
	}

	for _, event := range page.Data {
		switch event.Type {
		case "charge.created":
			_, err = statement.Exec(
				// TODO: deserialize to proper charge object
				event.Data.Obj["id"].(string),
				uint64(event.Data.Obj["amount"].(float64)),
				time.Unix(int64(event.Data.Obj["created"].(float64)), 0),
				// TODO: should actually be in its own table
				event.Sequence,
			)
		}
	}

	_, err = statement.Exec()
	if err != nil {
		return err
	}

	err = statement.Close()
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	log.Printf("Loaded page of %v event(s) in %v.",
		len(page.Data), time.Now().Sub(startPage))
	return nil
}

func requestEvents(stripeKey, stripeURL string, doneChan chan int, pageChan chan Page) error {
	var sequence uint64
	client := &http.Client{}
	numProcessed := 0

	for {
		startPage := time.Now()

		url := fmt.Sprintf("%s/v1/events?sequence=%v", stripeURL, sequence)
		log.Printf("Requesting page: %v (sequence %v)", url, sequence)

		req, err := http.NewRequest("GET", url, nil)
		req.SetBasicAuth(stripeKey, "")
		resp, err := client.Do(req)
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

		log.Printf("Received page of %v event(s) in %v. Work queue depth is %v",
			len(page.Data), time.Now().Sub(startPage), len(pageChan))

		pageChan <- page

		numProcessed += len(page.Data)
		if !page.HasMore {
			break
		}

		// Set sequence for the next page request.
		if len(page.Data) > 0 {
			sequence = page.Data[len(page.Data)-1].Sequence
		}
	}

	doneChan <- numProcessed

	return nil
}
