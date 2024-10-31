SERVICE_NAME_PATHWAY = pathway


build:
	chmod +x ./debezium/connector.sh
	docker compose up -d
	docker compose exec debezium ./connector.sh

stop:
	docker compose down -v

connect-pathway:
	docker compose exec $(SERVICE_NAME_PATHWAY) bash

