# Notas de dependencias: MetadataService (#35)

Qué hace Ray y qué hace la aplicación en el servicio de metadatos del prototipo.
Es insumo para el mapa de dependencias del middleware (#38).

- Código: [`ray_proto/metadata_actor.py`](../../ray_proto/metadata_actor.py) (actor) y
  [`ray_proto/metadata_log.py`](../../ray_proto/metadata_log.py) (WAL + snapshot).
- Pruebas sin Ray: [`ray_proto/tests/test_metadata_actor.py`](../../ray_proto/tests/test_metadata_actor.py).
- Pruebas con Ray (se omiten si no está instalado):
  [`ray_proto/tests/test_metadata_ray.py`](../../ray_proto/tests/test_metadata_ray.py).
- Evidencia con Ray obtenida el 25 nov 2026: Ray 2.60.1, Python 3.13.2, macOS arm64, un
  solo nodo local. 34 tests en verde en dos pasadas seguidas.

```bash
python3 -m venv .venv
```
```bash
.venv/bin/python -m pip install "ray==2.60.1"
```
```bash
.venv/bin/python -m unittest discover -s ray_proto/tests -t . -v
```

## Qué sustituye

En `native-v1` los metadatos viven en PostgreSQL (`gateway/app/db.py`, `gateway/app/metadata.py`).
En `ray-prototype` no se usa PostgreSQL: el estado está en la memoria de un único actor y
cada mutación se registra en disco antes de confirmarse. Los nombres de métodos y de campos
son los de `MetadataRepository`, para que el driver (#32) se parezca al gateway nativo.

## Qué hizo Ray y qué hizo la aplicación

| Necesidad | Qué hace Ray | Qué hace la aplicación | Evidencia |
|---|---|---|---|
| Serializar commits (antes `SELECT … FOR UPDATE`) | Ejecuta las llamadas a un actor síncrono de una en una (`max_concurrency=1`) | Comprueba las precondiciones dentro de la misma llamada (`If-Match`, `If-None-Match`, objeto borrado, `min_remaining`) | `commit_version`, `remove_replica`; tests `test_conditional_writes`, `test_commit_after_concurrent_delete_conflicts`, `test_replica_guards_match_the_native_repository` |
| Durabilidad de una mutación | Nada | Escribe el registro en `wal.log` con `fsync` **antes** de aplicarlo y de responder; si falla, la memoria no cambia y el registro se deshace | `MetadataLog.append`, `_MetadataService._mutate`; test `test_failed_fsync_changes_neither_memory_nor_disk` |
| Reconstruir el estado tras un reinicio | Vuelve a crear el proceso y llama otra vez a `__init__` con los mismos argumentos (`max_restarts=-1`) | Carga `snapshot.json` y reaplica el WAL posterior | `_MetadataService.__init__`, `MetadataLog.recover`; tests `test_restart_rebuilds_identical_state_from_the_wal`, `test_restart_from_snapshot_plus_newer_wal_records`. Reinicio real con Ray: `test_killed_actor_is_restarted_by_ray_and_rebuilds_state_from_disk` tras `ray.kill(no_restart=False)`: otro proceso (pid 10439 → 10444), 1,55–1,63 s hasta volver a responder, 2 registros reaplicados en 0,17 ms y `dump_state()` idéntico |
| Snapshot sin aplicar dos veces | Nada | Snapshot atómico (tmp + `fsync` + `os.replace` + `fsync` del directorio), WAL nuevo vacío y filtro `seq <= last_seq` | `MetadataLog.write_snapshot`; tests `test_automatic_snapshot_empties_the_wal`, `test_crash_between_snapshot_and_wal_reset_does_not_apply_twice` |
| Escritura cortada a mitad | Nada | CRC32 por línea; una última línea incompleta o inválida se trunca (nunca se confirmó); una línea inválida con líneas válidas detrás es corrupción y se falla cerrado | `MetadataLog._read_wal`; tests `test_torn_last_record_is_discarded`, `test_complete_last_line_with_bad_crc_is_discarded`, `test_corruption_before_valid_records_fails_closed`, `test_corrupt_snapshot_fails_closed` |
| Reintentos | Con `max_task_retries=0` **no** reintenta: una llamada interrumpida por la caída del actor devuelve error al llamante | `commit_version(request_id=…)` es idempotente: un reintento del mismo PUT devuelve la versión ya confirmada y no crea otra. El blob del reintento queda sin referencias y lo recolecta el GC | tests `test_retry_with_same_request_id_does_not_create_a_second_version`, `test_request_ids_survive_a_restart` |
| Descubrimiento del servicio | Actor con nombre y namespace (`p16-metadata` en `p16`), `get_if_exists=True`, `ray.get_actor` | Fija el nombre y lo expone en `get_or_create_metadata_service` / `get_metadata_service` | test `test_actor_is_discoverable_by_name` (pasa con Ray 2.60.1) |
| Transporte de argumentos y errores | Serializa argumentos y resultados; re-lanza las excepciones del actor como subclase de la original | Define `CommitConflict` y `PreconditionFailed` | test `test_application_errors_keep_their_type_through_ray` (pasa con Ray 2.60.1: `except CommitConflict` funciona en el driver) |
| Arranque de los workers | Lanza los procesos worker; con `runtime_env` los lanza con `bash -c "exec <python> …"` **sin comillas** | Pasa `PYTHONPATH` por el entorno heredado (sin `runtime_env`) | Con `runtime_env={"env_vars": …}` y el repo en `…/Redes Distribuida/`, `raylet.err` muestra `exec: /Users/…/Redes: cannot execute`; el actor nunca arranca y Ray lo da por muerto (`ACTOR_UNSCHEDULABLE_ERROR`) tras 5 intentos de un minuto. Supuesto de Ray: **rutas sin espacios** si se usa `runtime_env` |
| Dónde corre el actor | Lo planifica en un nodo con recursos libres; tras un reinicio puede elegir otro nodo | El directorio del WAL es local: en un clúster de varios nodos hay que fijarlo con un recurso propio (`resources={"p16_metadata": 1}` y `ray start --resources`) | `get_or_create_metadata_service(resources=…)`. **TODO**: no probado en varios nodos |
| Identificadores | Nada | ULID de `gateway/app/ids.py` (cargado por ruta, sin importar FastAPI/asyncpg) | `_load_native_ids` |
| Invariantes al arrancar | Nada | Una réplica sólo puede existir para un objeto `ACTIVE` y no hay dos objetos vivos con el mismo nombre; si no se cumple, no arranca | `_verify_invariants`, `_load_state` |

## Qué se pierde al reiniciar el actor

- **Nada de lo confirmado**, mientras sobreviva el disco del actor: toda mutación que
  devolvió respuesta está en el WAL o en el snapshot.
- **Llamadas en vuelo**: si el actor muere durante una llamada, el llamante recibe un error
  de Ray y no sabe si la mutación llegó al WAL. Para un PUT, el driver reintenta con el
  mismo `request_id`. El resto de mutaciones son seguras de repetir porque comprueban el
  estado antes de cambiarlo (como mucho, `fail_deletion` cuenta un intento de más y un
  `mark_deleted` repetido devuelve `None` porque el objeto ya no existe).
- **Salud de los nodos** (`HEALTHY` / `SUSPECT` / `DOWN`, fallos consecutivos): no se
  guarda; el detector de fallos la vuelve a calcular con sus sondeos. Sí se guardan la
  membresía y el peso (`upsert_node`).
- **Llamadas que llegan mientras se reinicia**: con Ray 2.60.1 fallan con
  `ActorUnavailableError` (no se encolan), así que el driver tiene que reintentarlas. El
  actor tardó 1,55–1,63 s en volver a responder; casi todo ese tiempo es Ray creando el
  proceso, porque reaplicar el WAL costó 0,17 ms. El coste de reaplicar crece con el WAL
  acumulado desde el último snapshot (`stats()["recovery"]["recovery_ms"]`). **TODO**:
  medirlo con un WAL grande.
- **Justo después de `ray.kill`** la primera llamada todavía puede contestarla el proceso
  viejo: matar el actor es asíncrono. El test espera a que responda un `pid` distinto.
- **Más allá de `max_remembered_requests`** (10 000 por defecto) se olvidan los
  `request_id` más antiguos: un reintento muy tardío crearía una versión nueva.

## Punto único de fallo declarado

Hay un solo `MetadataService` y su directorio está en el disco del nodo Ray donde corre.
Si se pierde ese disco o ese nodo, se pierden todos los metadatos, aunque los blobs sigan
en los StorageNode. Reconstruir los metadatos desde los sidecars de los blobs es el
issue #10 y queda fuera de alcance.

Además, como todas las llamadas (también las lecturas) pasan por un solo actor que
atiende de una en una, es un cuello de botella de rendimiento.

## Diferencias con native-v1

- Los tiempos son `float` (epoch en segundos), no `datetime`.
- Un objeto borrado queda como lápida en memoria (como la fila `DELETED` en PostgreSQL),
  así que el estado crece con los borrados hasta que haya compactación.
- No se comprueba que `node_ids` sean nodos registrados (en PostgreSQL había clave
  foránea a `nodes`).
- `list_objects` ordena con la comparación de cadenas de Python; PostgreSQL usa la
  colación de la base de datos y puede ordenar distinto con mayúsculas o acentos.
- No hay equivalente del *advisory lock* del reconciliador (`RECONCILER_LOCK`): si
  hubiera dos drivers reconciliando, nada los excluye. Queda como pregunta abierta.

## Por acordar con Ronald (driver, #32)

1. `commit_version(..., request_id=...)`: un `request_id` por PUT del cliente, el mismo
   en los reintentos y un `blob_id` nuevo en cada intento.
2. Tiempos como `float` epoch.
3. Nombre y namespace del actor: `p16-metadata` / `p16`.
4. Si la membresía (`upsert_node`, peso, `DRAINING`) se guarda aquí o la lleva el driver.

## Preguntas para la versión nativa (Fase 2)

- ¿Quién serializa los commits sin un actor (lock, líder, una sola cola)?
- ¿Cómo se reinicia el proceso de metadatos y cómo lo encuentran los demás sin nombres de Ray?
- ¿Se replica el WAL a otro nodo para eliminar el punto único de fallo?
- ¿Cómo se excluyen dos reconciliadores sin el *advisory lock* de PostgreSQL?
