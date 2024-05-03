up:
	docker-compose up -d

rebuild:
	docker-compose down
	docker-compose up -d --build

prune:
	docker-compose down
	docker container prune -f
	docker image prune -af