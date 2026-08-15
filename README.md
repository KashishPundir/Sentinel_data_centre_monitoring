# SentinelDC

SentinelDC monitors data-centre telemetry and forecasts module temperatures
five minutes ahead.  It validates incoming messages, records every forecast,
evaluates module-specific risk thresholds, and creates one alert per risk
transition (including recovery), rather than paging repeatedly for an
unchanged condition.

## Runtime flow

`Telemetry -> Kafka -> validated ML worker -> XGBoost forecast -> decision engine -> SQLite audit store -> API/dashboard`

The Kafka worker uses manual commits. An offset is committed only after all
module decisions are stored; if the worker crashes, Kafka redelivers it and
the stored partition/offset/module tuple prevents duplicate forecasts.

## Run locally

1. Install packages: `python -m pip install -r requirements.txt`
2. Start Kafka: `docker compose -f docker/kafka/docker-compose.yml up -d`
3. Start the monitoring worker: `python replay/kafka_consumer.py`
4. In another shell, start the API: `uvicorn api.app:app --host 0.0.0.0 --port 8000`
5. Open `/docs`, `/health/live`, `/health/ready`, or start the replay through
   the simulation endpoints.

## Deployment configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `SENTINELDC_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka bootstrap server |
| `SENTINELDC_TOPIC` | `datacenter_telemetry_v2` | Telemetry topic |
| `SENTINELDC_CONSUMER_GROUP` | `sentineldc-ml-consumer-v1` | Kafka consumer group |
| `SENTINELDC_ADMIN_API_KEY` | unset | Protects simulation control endpoints via `X-API-Key` |
| `SENTINELDC_LOG_LEVEL` | `INFO` | Worker log level |

For a multi-instance production deployment, move the audit store from SQLite
to managed PostgreSQL and configure Kafka authentication/TLS. The supplied
SQLite configuration uses WAL and a busy timeout so the single-node API and
worker can operate concurrently.
