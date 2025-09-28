SHELL := /bin/bash

default: docker-compose-up

docker-compose-up:
	docker compose -f docker-compose.yml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yml stop -t 1
	docker compose -f docker-compose.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs
