package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/Shopify/sarama"
	"github.com/joeshaw/envdecode"
)

var (
	// Number of seconds to wait while consuming a topic before assuming that
	// the topic is now empty and returning with what we have.
	ConsumeTimeout = 3

	// Default limit of events to return unless the user overrides.
	DefaultLimit = 10000
)

type Conf struct {
	KafkaTopic string `env:"KAFKA_TOPIC,required"`
	SeedBroker string `env:"SEED_BROKER,default=localhost:9092"`
}

type Page struct {
	Data    []*map[string]interface{} `json:"data"`
	HasMore bool                      `json:"has_more"`
	Object  string                    `json:"object"`
	URL     string                    `json:"url"`
}

func main() {
	var conf Conf
	err := envdecode.Decode(&conf)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumer(strings.Split(conf.SeedBroker, ","), nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	listEvents := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := DefaultLimit
		if r.Form.Get("limit") != "" {
			var err error
			limit, err = strconv.Atoi(r.Form.Get("limit"))
			if err != nil {
				log.Fatalln(err)
			}
		}

		// The one problem with this system is that the client always gets a
		// duplicate event when requesting the next page. We should probably
		// skip the initial event for convenience.
		offset := sarama.OffsetOldest
		if r.URL.Query().Get("sequence") != "" {
			var err error
			offset, err = strconv.ParseInt(r.URL.Query().Get("sequence"), 10, 64)
			if err != nil {
				log.Fatalln(err)
			}
		}

		log.Printf("Handling request limit %v offset %v", limit, offset)

		partitionConsumer, err := consumer.ConsumePartition(conf.KafkaTopic, 0, offset)
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()

		var events []*map[string]interface{}
		firstLoop := true
		hasMore := true

	ConsumerLoop:
		for {
			select {
			case message := <-partitionConsumer.Messages():
				// skip the first message due to overlap
				if offset != sarama.OffsetOldest && firstLoop {
					firstLoop = false
					break
				}

				var event map[string]interface{}
				err := json.Unmarshal(message.Value, &event)
				if err != nil {
					log.Fatalln(err)
				}

				// Fill the event's new `sequence` field (the public name for
				// "offset" in order to disambiguate from Stripe's old
				// offset-style pagination parameter).
				event["sequence"] = message.Offset

				events = append(events, &event)
				//log.Printf("Consumed message. Now have %v event(s).", len(events))

				// We've fulfilled the requested limit. We're done!
				if len(events) >= limit {
					break ConsumerLoop
				}

			// Unfortunately saram doesn't currently give us a good way of
			// detecting the end of a topic, so detect the end by timing out
			// for now.
			//
			// Note that this could result in a degenerate request which is
			// very long as new messages continue to trickle in until we hit
			// max page size at a rate that's never quite enough to hit our
			// timeout.
			case <-time.After(time.Second * time.Duration(ConsumeTimeout)):
				log.Printf("Timeout. Probably at end of topic.\n")
				hasMore = false
				break ConsumerLoop
			}
		}

		page := &Page{
			Data:    events,
			HasMore: hasMore,
			Object:  "list",
			URL:     "/v1/events",
		}

		data, err := json.Marshal(page)
		if err != nil {
			log.Fatalln(err)
		}

		w.Write(data)
		log.Printf("Responded to client with %v event(s)\n", len(events))
	})

	listEventsGz := gziphandler.GzipHandler(listEvents)
	http.Handle("/v1/events", listEventsGz)

	log.Printf("Starting HTTP server")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
