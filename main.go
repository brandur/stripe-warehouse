package main

import (
	"database/sql"
	"log"
	"strconv"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"
	"github.com/stripe/stripe-go"
	"github.com/stripe/stripe-go/charge"
	"github.com/stripe/stripe-go/event"
)

const (
	PageSize           = 100
	ReportingIncrement = 100
)

type Charge struct {
	ID      string    `gorm:"id"`
	Amount  uint64    `gorm:"amount"`
	Created time.Time `gorm:"created"`
}

type Conf struct {
	DatabaseURL string `env:"DATABASE_URL,required"`
	StripeKey   string `env:"STRIPE_KEY,required"`
}

func main() {
	var conf Conf
	err := envdecode.Decode(&conf)
	if err != nil {
		log.Fatal(err)
	}

	stripe.Key = conf.StripeKey
	//stripe.LogLevel = 1 // errors only

	dbSQL, err := sql.Open("postgres", conf.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}

	db, err := gorm.Open("postgres", dbSQL)
	if err != nil {
		log.Fatal(err)
	}

	//
	// Phase 0: Sample the log
	//
	// Capture a sample of the most recent event in our log. This will allow us
	// to start consuming the feed from after where our snapshot left off.
	//

	log.Printf("STEP: Sampling the log")
	eventSample, err := getEventSample()

	//
	// Phase 1: Load a snapshot
	//
	// Load a base snapshot of data as it stands in the API. This may be stale
	// by the time we iterate to its end but that's okay because we'll get
	// updates by using the log.
	//

	log.Printf("STEP: Loading from snapshot")
	err = loadSnapshot(db)
	if err != nil {
		log.Fatal(err)
	}

	//
	// Phase 2: Tail the log
	//
	// Consume the log from the position of the sample that we took before
	// loading the snapshot. This allows us to get all updates that occurred
	// during the snapshot loading process.
	//

	log.Printf("STEP: Tailing the log from %v", eventSample.ID)
	err = tailLog(db, eventSample)
	if err != nil {
		log.Fatal(err)
	}
}

func convertCharge(charge *stripe.Charge) *Charge {
	return &Charge{
		ID:      charge.ID,
		Amount:  charge.Amount,
		Created: time.Unix(charge.Created, 0),
	}
}

func getEventSample() (*stripe.Event, error) {
	params := &stripe.EventListParams{}
	params.Filters.AddFilter("limit", "", "1")
	iterator := event.List(params)

	for iterator.Next() {
		return iterator.Event(), nil
	}
	err := iterator.Err()
	return nil, err
}

func loadSnapshot(db gorm.DB) error {
	params := &stripe.ChargeListParams{}
	params.Filters.AddFilter("limit", "", strconv.Itoa(PageSize))

	var apiDuration, dbDuration time.Duration
	numProcessed := 0
	totalStart := time.Now()

	iterator := charge.List(params)
	timeIterator := func() bool {
		start := time.Now()
		ret := iterator.Next()
		apiDuration = apiDuration + time.Now().Sub(start)
		return ret
	}

	for timeIterator() {
		start := time.Now()
		charge := convertCharge(iterator.Charge())
		db.FirstOrCreate(&charge)
		dbDuration = dbDuration + time.Now().Sub(start)

		if db.Error != nil {
			return db.Error
		}

		numProcessed = numProcessed + 1
		if numProcessed%ReportingIncrement == 0 {
			log.Printf("Working. Processed %v record(s).", ReportingIncrement)
		}
	}
	if err := iterator.Err(); err != nil {
		return err
	}

	log.Printf("")
	log.Printf("== Snapshot Loading Results")
	log.Printf("API duration:            %v", apiDuration)
	log.Printf("DB duration:             %v", dbDuration)
	log.Printf("Total duration:          %v", time.Now().Sub(totalStart))
	log.Printf("Total records processed: %v", numProcessed)
	return nil
}

func tailLog(db gorm.DB, eventSample *stripe.Event) error {
	// Start paging from after the event sample that we capture before we
	// started loading from snapshot.
	params := &stripe.EventListParams{}
	params.Filters.AddFilter("limit", "", strconv.Itoa(PageSize))
	params.Filters.AddFilter("starting_after", "", eventSample.ID)

	var apiDuration, dbDuration time.Duration
	numProcessed := 0
	totalStart := time.Now()

	iterator := event.List(params)
	timeIterator := func() bool {
		start := time.Now()
		ret := iterator.Next()
		apiDuration = apiDuration + time.Now().Sub(start)
		return ret
	}

	for timeIterator() {
		start := time.Now()
		event := iterator.Event()

		switch event.Type {
		case "charge.created":
			charge := Charge{
				// TODO: deserialize to proper charge object
				ID:      event.Data.Obj["id"].(string),
				Amount:  event.Data.Obj["amount"].(uint64),
				Created: time.Unix(event.Data.Obj["created"].(int64), 0),
			}
			db.FirstOrCreate(&charge)
			dbDuration = dbDuration + time.Now().Sub(start)
			if db.Error != nil {
				return db.Error
			}

			numProcessed = numProcessed + 1
			if numProcessed%ReportingIncrement == 0 {
				log.Printf("Working. Processed %v record(s).", ReportingIncrement)
			}
		}
	}
	if err := iterator.Err(); err != nil {
		return err
	}

	log.Printf("")
	log.Printf("== Log Tailing Results")
	log.Printf("API duration:            %v", apiDuration)
	log.Printf("DB duration:             %v", dbDuration)
	log.Printf("Total duration:          %v", time.Now().Sub(totalStart))
	log.Printf("Total records processed: %v", numProcessed)
	return nil
}
