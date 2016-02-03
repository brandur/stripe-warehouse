# Stripe Warehouse

A work-in-progress demonstration of export capabilities with the Stripe API.

Export the name of a Kafka topic (I usually suffix them because deleting and
re-creating topics can be quite painful):

    export KAFKA_TOPIC=stripe-events-0

Create a Kafka topic with a compacted log:

    kafka-topics.sh --zookeeper localhost:2181 --create --topic $KAFKA_TOPIC --partitions 1 --replication-factor 1 --config cleanup.policy=compact

You can go into Zookeeper and verify your config:

    zkCli
    get /config/topics/$KAFKA_TOPIC
