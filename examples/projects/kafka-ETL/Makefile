build:
	docker compose up -d

stop:
	docker compose down -v --remove-orphans
	docker rmi etl-kafka-pathway:latest 
	docker rmi etl-kafka-stream-producer:latest 

connect:
	docker compose exec -it pathway bash
connect-prod:
	docker compose exec -it stream-producer bash
connect-kafka:
	docker compose exec -it kafka bash

logs:
	docker compose logs pathway
logs-prod:
	docker compose logs stream-producer
logs-kafka:
	docker compose logs kafka
logs-zookeeper:
	docker compose logs zookeeper
