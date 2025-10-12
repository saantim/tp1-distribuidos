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
  dataset: "minimal"  # o "full"

# Transformers (parsean CSVs)
transformers:
  menu_items:
    module: "menu"
    replicas: 1
    input:
      queue: "raw_menu_items_batches"
    output:
      exchange: "menu_items_source"
      strategy: "FANOUT"
      routing_keys: ["common"]

# Pipelines de queries
queries:
  q1:
    description: "Filtrar por año → hora → monto"
    enabled: true
    stages:
      filter_year:
        type: "filter"
        module: "year"
        replicas: 3
        input:
          queue: "transactions_source"
        output:
          queues: ["q1.filtered_year", "q4.filtered_year"]
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
  queues: ["q1.output", "q2.output"]
```
