SHELL := /bin/bash

default: docker-compose-up

generate-compose:
	python3 generate_compose.py
.PHONY: generate-compose

docker-compose-up: generate-compose
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
	@docker compose -f docker-compose.yml ps --services | grep -E '^(filter_tx_2024_2025_|filter_tx_6am_11pm_|filter_amount_|sink_q1_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q1

logs-q2:
	@docker compose -f docker-compose.yml ps --services | grep -E '^(filter_tx_item_2024_2025_|aggregator_period_|merger_period_|enricher_item_|sink_q2_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q2

logs-q3:
	@docker compose -f docker-compose.yml ps --services | grep -E '^(semester_aggregator_|merger_semester_results_|enricher_semester_tx_|sink_q3_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q3

logs-q4:
	@docker compose -f docker-compose.yml ps --services | grep -E '^(router_|aggregator_user_purchase_|top3_users_aggregator_|store_enricher_tx_|user_enricher_tx_|merger_final_top3_|sink_q4_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q4

logs-client:
	docker compose -f docker-compose.yml logs client gateway -f
.PHONY: logs-client
