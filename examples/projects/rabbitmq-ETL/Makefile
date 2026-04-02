build:
	docker compose up -d

stop:
	docker compose down -v --remove-orphans
	docker rmi etl-rabbitmq-pathway:latest
	docker rmi etl-rabbitmq-stream-producer:latest

connect:
	docker compose exec -it pathway bash
connect-prod:
	docker compose exec -it stream-producer bash
connect-rabbitmq:
	docker compose exec -it rabbitmq bash
psql:
	docker compose exec -it postgres psql -U pathway -d etl_db

logs:
	docker compose logs pathway
logs-prod:
	docker compose logs stream-producer
logs-rabbitmq:
	docker compose logs rabbitmq
logs-postgres:
	docker compose logs postgres
