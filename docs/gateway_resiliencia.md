# Gateway - Resiliencia

## Objetivo

El gateway puede caerse y volver. El cliente debe poder reconectarse y continuar sin perder datos.

## Supuestos

- El cliente nunca falla
- El cliente es "inteligente": puede reconectarse y reenviar datos no confirmados
- Solo hay un gateway (single instance + health-checker)
- El health-checker mantiene el pipeline vivo, no hay timeout de resultados

## Escenarios de fallo

**Escenario 1:** Gateway muere mientras recibe CSVs del cliente
- Algunos batches llegaron a RabbitMQ, otros no
- Cliente no sabe cuales llegaron

**Escenario 2:** Gateway muere mientras espera resultados
- Workers estan procesando o ya terminaron
- Resultados en RabbitMQ no fueron entregados al cliente

## Solucion

### Heartbeat al health-checker

El gateway debe enviar heartbeats UDP al health-checker igual que los workers. Agregar el thread de heartbeat existente (`HeartbeatSender`) al gateway para que pueda ser monitoreado y revivido.

### Persistencia en gateway

El gateway persiste a disco:
- Estado de cada sesion (fase actual, ultimo batch confirmado)
- Resultados recibidos de cada query

```
.gateway_state/
└── <session_id>/
    ├── meta.json
    └── results/
        ├── q1.json
        ├── q2.json
        └── ...
```

### Protocolo cliente-gateway

**Nueva conexion:**
```
Cliente -> Gateway:  NEW_SESSION
Gateway -> Cliente:  SESSION_ID(uuid)
```

**Reconexion:**
```
Cliente -> Gateway:  RECONNECT(session_id)
Gateway -> Cliente:  SESSION_STATE(phase, last_acked_batch)
                     o SESSION_UNKNOWN si no existe
```

**Envio de datos (con ACK):**

El paquete `Batch` existente se extiende con un `batch_id` incremental.

```
Cliente -> Gateway:  BATCH(batch_id, entity_type, data)
Gateway:             persiste batch_id a disco
Gateway:             envia a RabbitMQ
Gateway -> Cliente:  BATCH_ACK(batch_id)
```

El cliente trackea el ultimo `batch_id` ACKeado. El gateway trackea el ultimo `batch_id` recibido por sesion.

**Resultados:**
```
Gateway:             recibe resultado de RabbitMQ
Gateway:             persiste a disco
Gateway:             ACK a RabbitMQ
Gateway -> Cliente:  RESULT(query_id, data)
Cliente -> Gateway:  RESULT_ACK(query_id)
Gateway:             puede limpiar resultado del disco
```

## Fases de una sesion

```
RECEIVING  ->  WAITING  ->  COMPLETE
   |              |             |
 recibiendo    esperando     todos los
 batches      resultados    resultados
                            entregados
```

## Flujo: fallo durante envio de datos

```
1. Cliente conecta, recibe session_id, guarda localmente
2. Cliente envia batch_1, recibe ACK, guarda last_ack=1
3. Cliente envia batch_2, recibe ACK, guarda last_ack=2
4. Cliente envia batch_3, gateway muere antes del ACK
5. Health-checker revive gateway
6. Gateway carga estado: session X, last_ack=2
7. Cliente detecta desconexion, reconecta con session_id
8. Gateway responde: phase=RECEIVING, last_acked=2
9. Cliente reenvia desde batch_3 y continua
```

## Flujo: fallo esperando resultados

```
1. Cliente termino de enviar datos, phase=WAITING
2. Gateway recibe resultado q1, persiste a disco, ACK a RabbitMQ
3. Gateway recibe resultado q2, persiste a disco, muere antes de enviar al cliente
4. Health-checker revive gateway
5. Gateway carga estado: tiene q1 y q2 en disco
6. Cliente reconecta con session_id
7. Gateway responde: phase=WAITING
8. Gateway envia q1 y q2 (pendientes) al cliente
9. Gateway sigue consumiendo de RabbitMQ para q3, q4
```

## Cliente

El cliente debe:
- Guardar el session_id recibido al inicio
- Mantener contador de batch_id incremental
- Trackear ultimo batch_id ACKeado por el gateway
- Al detectar desconexion: reconectar enviando session_id
- Segun respuesta del gateway:
  - Si phase=RECEIVING: reenviar desde last_acked+1
  - Si phase=WAITING: esperar resultados
  - Si SESSION_UNKNOWN: empezar sesion nueva

## Componentes a modificar

**gateway/**
- Agregar `HeartbeatSender` para ser monitoreado por health-checker
- session_manager.py: agregar persistencia a disco
- protocol handling: soportar RECONNECT y SESSION_STATE
- result_collector.py: persistir resultado antes de ACK a RabbitMQ

**client/**
- Agregar batch_id incremental al enviar
- Trackear ultimo ACK recibido
- Logica de reconexion con session_id
- Reenvio de batches desde last_acked+1

**shared/protocol.py:**
- Extender `Batch` con campo `batch_id`
- Agregar `RECONNECT` packet
- Agregar `SESSION_STATE` packet
- Agregar `BATCH_ACK` packet
- Agregar `RESULT_ACK` packet

## Estructura de meta.json

```json
{
  "session_id": "uuid",
  "phase": "RECEIVING",
  "last_acked_batch": 42,
  "expected_queries": ["q1", "q2", "q3", "q4"],
  "received_queries": ["q1", "q2"]
}
```

## Limpieza

Cuando una sesion esta COMPLETE y el cliente confirmo todos los resultados (RESULT_ACK para cada query), el gateway borra el directorio de esa sesion del disco.
