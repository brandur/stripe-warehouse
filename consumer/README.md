# Stripe Warehouse Naive Implementation

This is a naive version of the warehouse that uses only currently available
capabilities like the charges and event endpoints. In practice, it will
probably be too slow to scale for large customers.

## Setup

``` sh
createdb stripe-warehouse
psql stripe-warehouse < db/structure.sql
```
