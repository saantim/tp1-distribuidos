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

## Estructura de Carpetas de Datos

El proyecto requiere las siguientes carpetas para funcionar correctamente:

### `.data/` - Datasets de Entrada

Contiene los archivos CSV que el sistema procesa. Deben existir dos datasets:

```
.data/
├── dataset_min/          # Dataset mínimo (para desarrollo y pruebas rápidas)
│   ├── menu_items/       # Items del menú (CSV)
│   ├── stores/           # Tiendas (CSV)
│   ├── transaction_items/# Items de transacciones (CSV)
│   ├── transactions/     # Transacciones (CSV)
│   └── users/            # Usuarios (CSV)
└── dataset_full/         # Dataset completo (para pruebas de producción)
    ├── menu_items/
    ├── stores/
    ├── transaction_items/
    ├── transactions/
    └── users/
```

**Nota**: Los datasets deben descargarse/generarse antes de ejecutar el sistema. El cliente lee estos archivos desde volúmenes montados en Docker.

### `.results/` - Resultados de Ejecución

Almacena los resultados generados por el sistema:

```
.results/
├── expected/             # Resultados esperados para validación (NO se eliminan con make clean_res)
│   ├── min/              # Resultados esperados para dataset_min
│   │   ├── q1.json
│   │   ├── q2.json
│   │   ├── q3.json
│   │   └── q4.json
│   └── full/             # Resultados esperados para dataset_full
│       ├── q1.json
│       ├── q2.json
│       ├── q3.json
│       └── q4.json
├── <session_id>/         # Resultados de cada sesión de cliente (UUID)
│   ├── q1.json
│   ├── q2.json
│   ├── q3.json
│   └── q4.json
└── reports/              # Reportes de validación
```

**Importante**:
- Los resultados de sesión se identifican por UUID único
- `make clean_res` elimina todas las carpetas excepto `expected/`
- Los resultados esperados deben generarse antes de poder validar

### `.kaggle/` - Scripts de Validación

Contiene los scripts Python para generar y validar resultados:

```
.kaggle/
├── build_expected.py     # Genera resultados esperados desde CSVs locales
└── validation.py         # Compara resultados de sesiones contra esperados
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

#### 1. Generar Resultados Esperados

Antes de validar, debes generar los resultados esperados procesando los CSVs localmente:

```bash
# Generar resultados esperados para dataset minimal
make gen_min

# Generar resultados esperados para dataset full
make gen_full
```

**¿Qué hace esto?**
- Ejecuta `.kaggle/build_expected.py` con Pandas localmente (sin Docker)
- Lee los CSVs desde `.data/dataset_min/` o `.data/dataset_full/`
- Genera archivos de resultados en `.results/expected/min/` o `.results/expected/full/`
- Crea archivos: `q1.json`, `q2.json`, `q3.json`, `q4.json`

**Importante**: Estos archivos NO se eliminan con `make clean_res` y sirven como "ground truth" para validaciones.

#### 2. Ejecutar el Sistema

```bash
# Generar compose e iniciar sistema
make

# El sistema procesará los datos y generará resultados en:
# .results/<session_id>/q1.json
# .results/<session_id>/q2.json
# .results/<session_id>/q3.json
# .results/<session_id>/q4.json
```

#### 3. Validar Resultados

Una vez que el sistema terminó de procesar, valida los resultados comparándolos con los esperados:

```bash
# Validar todas las queries de la última sesión (dataset minimal)
make valid_min

# Validar una sesión específica
make valid_min SESSION=e2baac91-147f-4b01-b0b8-5e0ca63af78e

# Validar queries específicas de la última sesión
make valid_min QUERIES=q1,q3

# Validar con dataset full
make valid_full

# Combinar opciones
make valid_min SESSION=<uuid> QUERIES=q1,q2
```

**¿Qué hace la validación?**
- Ejecuta `.kaggle/validation.py`
- Compara línea por línea los resultados en `.results/<session_id>/` vs `.results/expected/`
- Genera reporte en `.results/reports/<session_id>_validation_report.json`
- Muestra en consola: ✅ (match) o ❌ (diferencias encontradas)

**Workflow típico de validación:**

```bash
# 1. Generar resultados esperados (solo una vez)
make gen_min

# 2. Limpiar resultados previos
make clean_res

# 3. Ejecutar sistema
make

# 4. Esperar a que termine el procesamiento
# (monitorear logs con make logs-client)

# 5. Validar resultados
make valid_min

# 6. Si falla, revisar diferencias en .results/reports/
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
