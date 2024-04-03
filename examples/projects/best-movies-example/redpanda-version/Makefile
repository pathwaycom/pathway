
build:
	docker compose up -d

stop:
	docker compose down -v
	docker rmi best-movies-redpanda-pathway:latest 
	docker rmi best-movies-redpanda-stream-producer:latest 

connect:
	docker compose exec -it pathway bash
connect-prod:
	docker compose exec -it stream-producer bash
connect-redpanda:
	docker compose exec redpanda-tuto bash

logs:
	docker compose logs pathway
logs-prod:
	docker compose logs stream-producer
logs-redpanda:
	docker compose logs redpanda-tuto


info:
	docker exec -it redpanda-tuto rpk cluster info
create-topic:
	docker exec -it redpanda-tuto rpk topic create values
create-message:
	docker exec -it redpanda-tuto rpk topic produce values