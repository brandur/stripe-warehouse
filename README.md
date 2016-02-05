# Stripe Warehouse

A work-in-progress demonstration of export capabilities with the Stripe API.

Install and start both Zookeeper and Kafka (this may be as simple as `brew
install zookeeper kafka` and copying some plist files around).

Export the name of a Kafka topic (I usually suffix them because deleting and
re-creating topics can be quite painful):

    export KAFKA_TOPIC=stripe-events-0

Create a Kafka topic with a compacted log:

    kafka-topics.sh --zookeeper localhost:2181 --create --topic $KAFKA_TOPIC --partitions 1 --replication-factor 1 --config cleanup.policy=compact

You can go into Zookeeper and verify your config:

    zkCli
    get /config/topics/$KAFKA_TOPIC

## Usage

Make sure that you have a `KAFKA_TOPIC` exported from above.

First synthesize some events and produce them into Kafka:

    export NUM_EVENTS=1000000
    cd synthesizer
    go build && ./synthesizer

Then start up the basic endpoint interface which will read out of Kafka:

    cd endpoint
    go build && ./endpoint

Then build your warehouse by consuming the HTTP interface that you just started
up (you will need to have Postgres installed and running for this step to
work):

    cd consumer
    createdb stripe-warehouse
    psql stripe-warehouse < db/structure.sql
    export DATABASE_URL=postgres://localhost/stripe-warehouse?sslmode=disable
    go build && ./consumer
