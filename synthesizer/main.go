package main

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"
	"github.com/stripe/stripe-go"
)

const (
	KafkaBatchSize     = 100
	ReportingIncrement = 10000
)

type Conf struct {
	KafkaTopic string `env:"KAFKA_TOPIC"`
	NumEvents  int    `env:"NUM_EVENTS"`
	SeedBroker string `env:"SEED_BROKER,default=localhost:9092"`
}

func main() {
	var conf Conf
	err := envdecode.Decode(&conf)
	if err != nil {
		log.Fatal(err)
	}

	producer, err := sarama.NewSyncProducer(strings.Split(conf.SeedBroker, ","), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("Synthesizing %v event(s)", conf.NumEvents)
	err = synthesizeEvents(producer, conf.KafkaTopic, conf.NumEvents)
	if err != nil {
		log.Fatal(err)
	}
}

// Note that unfortunately this does not actually produce in batches yet. We
// should theoretically be able to with Kafka, but the sarama interface for a
// `SyncProducer` currently seems overly limited.
func processBatch(producer sarama.SyncProducer, topic string, events []*stripe.Event) error {
	for _, event := range events {
		data, err := json.Marshal(event)
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

		//start := time.Now()
		//partition, offset, err := producer.SendMessage(message)
		_, _, err = producer.SendMessage(message)
		if err != nil {
			return err
		} else {
			//log.Printf("> Message sent to partition %d at offset %d in %v\n",
			//partition, offset, time.Now().Sub(start))
		}
	}

	return nil
}

func synthesizeEvents(producer sarama.SyncProducer, topic string, numEvents int) error {
	var batch []*stripe.Event
	for i := 0; i < numEvents; i++ {
		charge := &stripe.Charge{
			Amount:         9900,
			AmountRefunded: 0,
			Captured:       true,
			Currency:       "usd",
			ID:             "ch_" + randomString(25),
			Live:           false,
			Paid:           true,
			Refunded:       false,
			Status:         "succeeded",
		}
		chargeData, err := json.Marshal(charge)
		if err != nil {
			return err
		}

		event := &stripe.Event{
			Created: 1454595354,
			Data: &stripe.EventData{
				// `Raw` is what's going to be sent over the wire here, but
				// also populate `id` under `Obj` so the producer can select an
				// appropriate key for it.
				Obj: map[string]interface{}{
					"id": charge.ID,
				},
				Raw: json.RawMessage(chargeData),
			},
			ID:       "evt_" + randomString(25),
			Live:     false,
			Type:     "charge.created",
			Webhooks: 0,
		}

		batch = append(batch, event)

		if len(batch) == KafkaBatchSize {
			err := processBatch(producer, topic, batch)
			if err != nil {
				return err
			}
			batch = nil
		}

		if (i+1)%ReportingIncrement == 0 {
			log.Printf("Working. Processed %v record(s).", i+1)
		}
	}
	return nil
}
