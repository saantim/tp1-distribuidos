# Distribuidos: TP Escalabilidad

---

## Requerimientos para Desarrollar

- Crear `venv` de Python ejecutando el siguiente comando:

```bash
python3.13 -m venv venv
```

- Inicializar `venv` con el comando:

```bash
source venv/bin/activate
```

- Por último, instalar dependencias necesarias:

```bash
pip3 install -r requirements.txt
```

## Comandos Principales

### Ejecución del Sistema

```bash
make                        # Generar docker-compose.yml e iniciar sistema
make docker-compose-down    # Detener y eliminar todos los contenedores
make docker-compose-logs    # Ver logs de todos los servicios
make clean_res              # Limpiar resultados (excepto .results/expected)
```

### Visualización de Logs

```bash
make logs-client            # Logs de cliente y gateway
make logs-chaos-monkey      # Logs de chaos monkey
make logs-q1                # Logs de pipeline Q1 (transformer_transactions + q1_*)
make logs-q2                # Logs de pipeline Q2 (transformers + q2_*)
make logs-q3                # Logs de pipeline Q3 (transformers + q3_*)
make logs-q4                # Logs de pipeline Q4 (transformers + q4_*)
make logs-qtest             # Logs de pipeline de testing
make logs-health            # Logs de health checkers
```

### Testing y Validación

```bash
# Generar resultados esperados
make gen_min                # Dataset minimal
make gen_full               # Dataset full

# Validar resultados
make valid_min              # Todas las queries, última sesión
make valid_min SESSION=<uuid>  # Sesión específica
make valid_min QUERIES=q1,q3   # Queries específicas
make valid_full             # Validación con dataset full
```

### Chaos Monkey

```bash
# Matar contenedores manualmente
make kill-containers                        # Mata todos los contenedores permitidos
make kill-containers prefixes=q1_,q2_       # Mata contenedores por prefijo
make kill-containers prefixes=q1_ loop=10   # Mata en loop cada 10 segundos
```

**Nota**: El chaos monkey también se ejecuta automáticamente como servicio en docker-compose si está habilitado en `compose_config.yaml`.

### Tests Múltiples Clientes

```bash
make multi_client_test      # Ejecutar con test_compose_config.yaml
```

## Configuración del Pipeline

La topología de docker-compose se genera desde `compose_config.yaml`.

### Uso

```bash
# Generar docker-compose.yml
python generate_compose.py

# O usar make (genera e inicia)
make
```

### Formato de Configuración

Editar `compose_config.yaml` para modificar el pipeline:

```yaml
# Configuración general
settings:
  chaos_monkey:
    enabled: true           # Habilitar/deshabilitar chaos monkey
    filter_prefix: "gateway,chaos_monkey,rabbitmq"  # Contenedores protegidos
    full:
      enabled: true         # Matar todos los contenedores permitidos
      interval: 90          # Intervalo en segundos
    single:
      enabled: true         # Matar contenedores individuales
      interval: 5
    start_delay: 30         # Delay antes de iniciar chaos monkey
  health_checker:
    enabled: true           # Habilitar health checkers
    replicas: 5             # Cantidad de health checkers

# Transformers (parsean CSVs)
transformers:
  menu_items:
    module: "menu"
    replicas: 1
    input: "raw_menu_items_batches"
    output:
      - name: "menu_items_source"
        routing_fn: "broadcast"

# Pipelines de queries
queries:
  q1:
    description: "Filtrar por año → hora → monto"
    enabled: true
    stages:
      filter_year:
        type: "filter"
        module: "year"
        replicas: 5
        input: "transactions_source"
        output:
          - name: "q1.filtered_year"
            downstream_stage: "q1_filter_hour"
            downstream_workers: 5
```

### Operaciones Comunes

**Escalar un stage:**
```yaml
replicas: 10  # Cambiar este número
```

**Habilitar/deshabilitar query:**
```yaml
queries:
  q2:
    enabled: false
```

**Compartir stage entre queries:**
```yaml
output:
  - name: "q1.output"
    downstream_stage: "q1_next"
    downstream_workers: 3
  - name: "q2.output"
    downstream_stage: "q2_next"
    downstream_workers: 5
```
