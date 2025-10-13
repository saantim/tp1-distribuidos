SHELL := /bin/bash

default: docker-compose-up

generate-compose:
	./venv/bin/python generate_compose.py
.PHONY: generate-compose

docker-compose-up: generate-compose
	docker compose -f docker-compose.yml up -d --build --force-recreate
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yml down -v
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs

logs-client:
	clear
	docker compose -f docker-compose.yml logs client gateway -f
.PHONY: logs-client

logs-q1:
	clear
	@docker compose -f docker-compose.yml ps --services | grep -E '^(transformer_transactions|q1_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q1

logs-q2:
	clear
	@docker compose -f docker-compose.yml ps --services | grep -E '^(transformer_transaction_items|transformer_menu_items|q2_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q2

logs-q3:
	clear
	@docker compose -f docker-compose.yml ps --services | grep -E '^(transformer_transactions|transformer_stores|q3_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q3

logs-q4:
	clear
	@docker compose -f docker-compose.yml ps --services | grep -E '^(transformer_transactions|transformer_stores|transformer_users|q4_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q4

gen_min:
	python3 .kaggle/build_expected.py --dataset min
.PHONY: gen_min

gen_full:
	python3 .kaggle/build_expected.py --dataset full
.PHONY: gen_full

valid_min:
	@if [ -n "$(SESSION)" ]; then \
		python3 .kaggle/validation.py --dataset min --session $(SESSION); \
	else \
		python3 .kaggle/validation.py --dataset min; \
	fi
.PHONY: valid_min

valid_full:
	@if [ -n "$(SESSION)" ]; then \
		python3 .kaggle/validation.py --dataset full --session $(SESSION); \
	else \
		python3 .kaggle/validation.py --dataset full; \
	fi
.PHONY: valid_full

clean_res:
	rm -rf .results/ pipeline/*.json
	@echo "Cleaned pipeline results"
.PHONY: clean_res
