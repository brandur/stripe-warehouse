# Stripe Warehouse Feeder

Iterates the Stripe `GET /v1/events` endpoint and produces each event into
Kafka.

    export KAFKA_TOPIC=
    export STRIPE_KEY=
    go build
    ./feeder
