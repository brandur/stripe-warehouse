package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/joeshaw/envdecode"
)

const (
	ReportingIncrement = 10000
)

type Conf struct {
	KafkaTopic string `env:"KAFKA_TOPIC"`
	SeedBroker string `env:"SEED_BROKER,default=localhost:9092"`
}

func main() {
	var conf Conf
	err := envdecode.Decode(&conf)
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumer([]string{conf.SeedBroker}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// start right at the beginning
	partitionConsumer, err := consumer.ConsumePartition(conf.KafkaTopic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case <-partitionConsumer.Messages():
			//case msg := <-partitionConsumer.Messages():
			//log.Printf("Consumed message offset %d\n", msg.Offset)
			//log.Printf("Message = %v\n", string(msg.Value))
			consumed++

			if consumed%ReportingIncrement == 0 {
				log.Printf("Working. Processed %v record(s).", consumed)
			}
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
