package main

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"
	"github.com/stripe/stripe-go"
	"github.com/stripe/stripe-go/event"
)

const (
	KafkaBatchSize     = 100
	PageSize           = 100
	ReportingIncrement = 100
)

type Conf struct {
	KafkaTopic string `env:"KAFKA_TOPIC"`
	SeedBroker string `env:"SEED_BROKER,default=localhost:9092"`
	StripeKey  string `env:"STRIPE_KEY,required"`
}

func main() {
	var conf Conf
	err := envdecode.Decode(&conf)
	if err != nil {
		log.Fatal(err)
	}

	stripe.Key = conf.StripeKey
	//stripe.LogLevel = 1 // errors only

	producer, err := sarama.NewSyncProducer(strings.Split(conf.SeedBroker, ","), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("Tailing the log")
	err = tailLog(producer, conf.KafkaTopic)
	if err != nil {
		log.Fatal(err)
	}
}

func processBatch(producer sarama.SyncProducer, topic string, events []*stripe.Event) error {
	for _, event := range events {
		data, err := json.Marshal(event.Data.Obj)
		if err != nil {
			return err
		}

		id := event.Data.Obj["id"]
		if id == nil {
			log.Printf("Found event with nil data ID, type is %v", event.Type)
		}

		// TODO: Verify that Kafka does indeed perform log compaction per
		// partition key (as opposed to some other type of "key"). The docs
		// aren't exactly clear on this point.
		key := ""
		if id != nil {
			key = id.(string)
		}

		message := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(data),
		}

		start := time.Now()
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			return err
		} else {
			log.Printf("> Message sent to partition %d at offset %d in %v\n",
				partition, offset, time.Now().Sub(start))
		}
	}

	return nil
}

func tailLog(producer sarama.SyncProducer, topic string) error {
	numProcessed := 0

	params := &stripe.EventListParams{}
	params.Filters.AddFilter("limit", "", strconv.Itoa(PageSize))

	iterator := event.List(params)
	timeIterator := func() bool {
		//start := time.Now()
		ret := iterator.Next()
		//log.Printf("API request took %v", time.Now().Sub(start))
		return ret
	}

	var batch []*stripe.Event
	for timeIterator() {
		event := iterator.Event()

		batch = append(batch, event)

		if len(batch) == KafkaBatchSize {
			err := processBatch(producer, topic, batch)
			if err != nil {
				return err
			}
			batch = nil

			numProcessed = numProcessed + 1
			if numProcessed%ReportingIncrement == 0 {
				log.Printf("Working. Processed %v record(s).", ReportingIncrement)
			}
		}
	}
	if err := iterator.Err(); err != nil {
		return err
	}
	return nil
}
