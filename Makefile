SHELL := /bin/bash

default: docker-compose-up

docker-compose-up:
	docker compose -f docker-compose.yml up -d --build --force-recreate
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yml stop -t 1
	docker compose -f docker-compose.yml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs


logs-q1:
	docker compose -f docker-compose.yml logs demux filter_tx_2024_2025 filter_tx_6am_11pm filter_amount sink_q1 -f

logs-q2:
	docker compose -f docker-compose.yml logs demux filter_tx_item_2024_2025 aggregator_period merger_period enricher_item sink_q2 -f

logs-q3:
	docker compose -f docker-compose.yml logs semester_aggregator merger_semester_results enricher_semester_tx sink_q3 -f
