SHELL := /bin/bash

default: docker-compose-up

generate-compose:
	python3 generate_compose.py
.PHONY: generate-compose

build_test_compose:
	python3 generate_compose.py test_compose_config.yaml

multi_client_test: docker-compose-down build_test_compose
	docker compose -f docker-compose.yml up -d
.PHONY: multi_client_test

docker-compose-up: clean_res generate-compose
	docker compose -f docker-compose.yml up -d
.PHONY: docker-compose-up

docker-compose-rebuild: clean_res generate-compose
	docker compose -f docker-compose.yml up -d
.PHONY: docker-compose-rebuild

docker-compose-down:
	docker compose -f docker-compose.yml down -v
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yml logs -f
.PHONY: docker-compose-logs

logs-client:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^(client_|gateway)' | xargs -r docker compose -f docker-compose.yml logs -f
.PHONY: logs-client

logs-chaos-monkey:
	clear
	@docker compose -f docker-compose.yml logs chaos_monkey -f
.PHONY: logs-client

logs-qtest:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^(transformer_transactions|q_testing_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q1

logs-q1:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^(transformer_transactions|q1_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q1

logs-q2:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^(transformer_transaction_items|transformer_menu_items|q2_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q2

logs-q3:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^(transformer_transactions|transformer_stores|q3_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q3

logs-q4:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^(transformer_transactions|transformer_stores|transformer_users|q4_)' | xargs docker compose -f docker-compose.yml logs -f
.PHONY: logs-q4

logs-health:
	clear
	@docker compose -f docker-compose.yml ps -a --services | grep -E '^health_checker_' | xargs -r docker compose -f docker-compose.yml logs -f
.PHONY: logs-health

gen_min:
	python3 .kaggle/build_expected.py --dataset min
.PHONY: gen_min

gen_full:
	python3 .kaggle/build_expected.py --dataset full
.PHONY: gen_full

valid_min:
	@ARGS="--dataset min"; \
	if [ -n "$(SESSION)" ]; then ARGS="$$ARGS --session $(SESSION)"; fi; \
	if [ -n "$(QUERIES)" ]; then ARGS="$$ARGS --queries $(QUERIES)"; fi; \
	python3 .kaggle/validation.py $$ARGS
.PHONY: valid_min

valid_full:
	@ARGS="--dataset full"; \
	if [ -n "$(SESSION)" ]; then ARGS="$$ARGS --session $(SESSION)"; fi; \
	if [ -n "$(QUERIES)" ]; then ARGS="$$ARGS --queries $(QUERIES)"; fi; \
	python3 .kaggle/validation.py $$ARGS
.PHONY: valid_full

test_count_eof:
	@echo "Contando mensajes 'flush_eof'..."
	@make logs-qtest | grep -c "flush_eof" | xargs echo "TOTAL flush_eof encontrados:"
.PHONY: count-flush

kill-containers:
	@ARGS=""; \
	if [ -n "$(prefixes)" ]; then ARGS="$$ARGS prefixes=$(prefixes)"; fi; \
	if [ -n "$(loop)" ]; then ARGS="$$ARGS loop=$(loop)"; fi; \
	docker compose exec chaos_monkey python /chaos_monkey/kill_script.py $$ARGS
.PHONY: kill-containers

clean_res:
	ls .results | grep -v '^expected$$' | xargs -I{} rm -rf .results/{}
	rm -rf .saved_sessions
	@echo "Cleaned pipeline results (except '.results/expected')"
.PHONY: clean_res
