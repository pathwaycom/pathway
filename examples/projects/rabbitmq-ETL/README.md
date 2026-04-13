# ETL with RabbitMQ Streams in / PostgreSQL out

A streaming ETL pipeline built with Pathway that reads from RabbitMQ Streams,
validates, transforms, aggregates, and joins data, then writes enriched results
to PostgreSQL in real time.

## What Pathway replaces

This example is inspired by three ETL architectures that traditionally require
many moving parts:

| Traditional approach | What Pathway replaces |
|---|---|
| Python/Java workers consuming from queues | `pw.io.rabbitmq.read` |
| Staging databases for validation | UDF-based filtering |
| Airflow DAGs for orchestration | Reactive streaming engine |
| Celery task queues for processing | Single streaming process |
| Separate aggregation jobs | `groupby().reduce()` |
| Multi-step load pipelines | `pw.io.postgres.write` |

**Result:** One Pathway process replaces 5+ services.

## Architecture

```
Producer (rstream)           Pathway (single process)              PostgreSQL
+----------------+    +------------------------------+    +------------------+
| employees_raw  |--->| pw.io.rabbitmq.read          |    |                  |
| orders_raw     |--->|   -> validate (UDFs)         |--->| enriched_employees|
| listings_raw   |--->|   -> transform               |    | enriched_listings |
+----------------+    |   -> aggregate (groupby)     |    | (snapshot mode)  |
                      |   -> join (left joins)        |    +------------------+
                      | pw.io.postgres.write           |
                      +------------------------------+
```

## Data sources

- **Employees** (10 records): HR data with intentional quality issues (empty names,
  invalid emails, negative salaries) to demonstrate validation
- **Orders** (15+ records): Purchase orders linked to employees by `employee_id`
- **Listings** (10+ records): Rental property listings linked to employees via
  `agent_employee_id` (inspired by rental platform crawlers)

## ETL pipeline

1. **Read** three RabbitMQ streams via `pw.io.rabbitmq.read`
2. **Validate** using UDFs: email format, phone format, positive salary/price, non-empty fields
3. **Transform**: normalize emails/departments to lowercase, parse string amounts to float,
   concatenate names, derive boolean flags
4. **Aggregate**: order stats per employee (`total_orders`, `total_spent`),
   listing stats per agent (`total_listings`, `avg_listing_price`)
5. **Join**: left-join employees with order stats and listing stats;
   left-join listings with agent names
6. **Write** to PostgreSQL snapshot tables (auto-updated as new data arrives)

## Launching

```bash
docker compose up -d
# or
make
```

## Checking results

Connect to PostgreSQL:

```bash
make psql
```

Then query the enriched tables:

```sql
SELECT * FROM enriched_employees ORDER BY employee_id;
SELECT * FROM enriched_listings ORDER BY listing_id;
```

As the producer continues sending new orders and listings, re-running these queries
will show updated aggregates in real time.

## Monitoring

- **Pathway logs**: `make logs`
- **Producer logs**: `make logs-prod`
- **RabbitMQ Management UI**: http://localhost:15672 (guest/guest)

## Stopping

```bash
make stop
```
