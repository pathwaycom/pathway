SERVICE_NAME_POSTGRES = postgres
SERVICE_NAME_PATHWAY = pathway
DB_NAME = values_db
USERNAME = user


build:
	chmod +x ./debezium/connector.sh
	chmod +x ./sql/update_db.sh
	docker compose up -d
	sleep 5
	docker compose exec debezium ./connector.sh
	docker compose exec postgres ./update_db.sh

stop:
	docker compose down -v
	# You may need to rename the image
	docker rmi debezium-postgres-example-pathway:latest

connect-bash:
	docker compose exec $(SERVICE_NAME_POSTGRES) bash

connect-psql:
	docker compose exec $(SERVICE_NAME_POSTGRES) psql values_db -U user -W
	
connect-pathway:
	docker compose exec $(SERVICE_NAME_PATHWAY) bash