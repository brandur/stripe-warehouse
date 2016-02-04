# Stripe Warehouse Synthesizer

Produces a series of fake event objects and produces them into Kafka.

    export KAFKA_TOPIC=
    export NUM_EVENTS=
    go build
    ./synthesizer
