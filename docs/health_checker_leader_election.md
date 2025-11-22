# Health Checker - Leader Election

## Objetivo

Correr N health checkers para tolerancia a fallos. Solo uno (el lider) revive containers. Si el lider muere, otro toma el control automaticamente.

## Regla de liderazgo

El lider es el HC con el **ID mas bajo entre los que estan vivos**, con una excepcion: si un HC muere y es revivido, queda como standby hasta que el lider actual muera.

Esto evita que un HC recien revivido (con estado incompleto) tome liderazgo inmediatamente.

## Roles

**LEADER:**
- Revive workers muertos
- Revive HCs muertos
- Un solo lider a la vez

**STANDBY:**
- Trackea workers (recibe heartbeats UDP)
- Trackea otros HCs (heartbeats TCP)
- No revive nada, solo observa

## Trackeo de workers

Todos los HCs (lider y standbys) reciben heartbeats UDP de workers y mantienen su propio registry. Esto permite takeover rapido: cuando el lider muere, el nuevo lider ya tiene el registry armado.

## Persistencia a disco

Cada HC hace flush de su worker registry a disco cada `check_interval` segundos.

Formato:
```json
{
  "q1_filter_0": 1700000000.123,
  "q1_filter_1": 1700000001.456,
  "transformer_transactions_0": 1700000002.789
}
```

Al arrancar, el HC carga el registry del disco si existe. Esto asegura que un HC revivido tenga un estado reciente (maximo `check_interval` segundos de antiguedad).

## Comunicacion entre HCs

- Protocolo: TCP
- Puerto: separado del UDP de workers (ej: 9091)
- Heartbeat interval: 2 segundos
- Timeout: 4 segundos

Cada HC envia heartbeats a todos los otros HCs y trackea cuales estan vivos.

## Flujo: lider muere

```
Estado inicial:
  HC_0: lider
  HC_1: standby
  HC_2: standby

HC_0 muere:
  HC_1 y HC_2 detectan ausencia de heartbeat (4s timeout)
  HC_1 se convierte en lider (ID mas bajo vivo)
  HC_1 hace "docker start health_checker_0"
  HC_0 vuelve como standby
```

## Flujo: nuevo lider muere rapido

```
HC_0 muere
HC_1 toma liderazgo, revive HC_0
HC_0 vuelve, carga registry del disco
HC_1 muere 3 segundos despues
HC_0 toma liderazgo con registry casi completo
```

Sin persistencia a disco, HC_0 tendria registry vacio y perderia workers.

## Componentes a implementar

```
health_checker/
├── hc_registry.py       # trackea heartbeats de otros HCs
├── hc_heartbeat.py      # cliente/servidor TCP para heartbeats entre HCs
├── leader_election.py   # determina si soy lider
├── registry.py          # trackea workers (existente, agregar persistencia)
└── server.py            # integrar todo, condicionar revival a liderazgo
```

## leader_election.py

```python
class LeaderElection:
    def __init__(self, my_id: int, hc_registry: HCRegistry):
        self.my_id = my_id
        self.hc_registry = hc_registry
        self.current_leader: int | None = None

    def am_i_leader(self) -> bool:
        alive_ids = self.hc_registry.get_alive_ids()
        alive_ids.append(self.my_id)

        # Si el lider actual sigue vivo, no cambiar
        if self.current_leader in alive_ids:
            return self.my_id == self.current_leader

        # Lider murio o no hay -> ID mas bajo toma
        self.current_leader = min(alive_ids)
        return self.my_id == self.current_leader
```

## Modificacion a server.py

```python
def _health_check_loop(self):
    while not self.shutdown_signal.should_shutdown():
        time.sleep(self.check_interval)

        # Flush registry a disco
        self.registry.persist()

        if not self.leader_election.am_i_leader():
            continue

        # Solo el lider revive
        for worker in self.registry.get_dead_workers(self.timeout_threshold):
            self._revive_container(worker.container_name)

        for hc_id in self.hc_registry.get_dead_hcs(self.hc_timeout):
            self._revive_container(f"health_checker_{hc_id}")
```

## Configuracion

```yaml
health_checker:
  replicas: 3
  port: 9090                  # UDP para workers
  hc_port: 9091               # TCP entre HCs
  heartbeat_interval: 5       # workers
  hc_heartbeat_interval: 2    # entre HCs
  check_interval: 10          # checkeo y flush a disco
  timeout_threshold: 15       # workers
  hc_timeout: 4               # entre HCs
```

## Variables de entorno por instancia

```
REPLICA_ID=0
REPLICAS=3
PORT=9090
HC_PORT=9091
HC_HEARTBEAT_INTERVAL=2
HC_TIMEOUT=4
CHECK_INTERVAL=10
TIMEOUT_THRESHOLD=15
```
