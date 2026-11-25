# P16 — Servicio distribuido de almacenamiento de objetos

Almacén de objetos estilo S3 (`clave + bytes + metadatos`) repartido entre varios
**Storage Nodes** independientes, con replicación, verificación de integridad SHA-256,
detección de fallos, reparación automática, reconciliación y cambios de membresía.

- **Stack**: Python 3.13 · FastAPI · PostgreSQL 17 · Docker Compose (sin Kafka, Redis ni Kubernetes).
- **Diseño y justificación** de cada decisión: [`docs/DISENO.md`](docs/DISENO.md).
- **Orientaci?n oficial**: [`docs/ORIENTACION.md`](docs/ORIENTACION.md).
- **Guion de la demo** para la defensa: [`docs/DEMO.md`](docs/DEMO.md).
- **Todos los comandos** (Docker, CLI, guiones, curl): [`docs/COMANDOS.md`](docs/COMANDOS.md).
- **Interfaz visual en vivo**: <http://localhost:8080/ui> (o `python3 scripts/p16.py watch`).

## Ramas y versiones

El trabajo se organiza por fases. Cada cambio se integra mediante un pull request dirigido a la rama de la versión correspondiente.

| Rama | Estado y propósito |
|---|---|
| `main` | Rama estable del repositorio; contiene la versión nativa actual y documentación compartida. |
| `native-v1` | Línea base nativa con FastAPI, PostgreSQL y HRW para comparación con Ray. Se conserva en el tag `baseline-postgres-2026-11` y está protegida. |
| `ray-prototype` | Rama de la Fase 1 para desarrollar y documentar el prototipo con Ray. |
| `native-v2` | Rama planificada para la Fase 2: rediseño nativo sin Ray. Se creará después de la entrega del prototipo. |

Los PR de la versión nativa de referencia deben dirigirse a `native-v1`; los del prototipo, a `ray-prototype`. Los cambios documentales generales pueden proponerse contra `main`.

```
Cliente ──HTTP──▶ Gateway :8080 ──▶ storage1 / storage2 / storage3 (+ storage4 opcional)
                     │                   (cada uno con su propio volumen Docker)
                     └──▶ PostgreSQL (metadatos: objetos, versiones, réplicas, nodos)
```

## Contrato que ofrece el sistema

| | |
|---|---|
| Ubicación | Rendezvous hashing ponderado (HRW) sobre el id estable (ULID) del objeto |
| Redundancia | 2 copias (RF=2) en nodos distintos, sin nodo primario |
| Durabilidad | Un PUT responde 2xx sólo tras escribir **2 copias con `fsync` y SHA-256 verificado** y confirmar los metadatos |
| Tolerancia | Sin pérdida de datos confirmados si se pierde 1 nodo; se puede leer mientras quede 1 copia válida y escribir mientras haya 2 nodos |
| Consistencia | Lectura-tras-escritura fuerte por clave; PUT concurrentes: gana el último (o `If-Match` / `If-None-Match`) |
| Integridad | SHA-256 al escribir, al leer, al copiar entre nodos y en un *scrub* periódico; nunca se sirven datos corruptos |
| Reparación | Reconciliación cada 10 s; re-replicación de un nodo caído tras 30 s; *read-repair* inmediato |

(Se puede consultar en vivo: `curl localhost:8080/cluster/config`).

---

## 1. Requisitos

- **Docker Desktop** (o Docker Engine) con Docker Compose v2.
- **Python 3.10+** en tu máquina, sólo para la CLI de ayuda, la demo y los tests (no necesita instalar paquetes).
- **curl** (opcional).
- Puerto **8080** libre.

## 2. Arrancar y parar

```bash
docker compose up -d --build --wait     # gateway + 3 storage nodes + PostgreSQL
docker compose ps                       # todos deben estar "healthy"
```

```bash
docker compose down                     # parar (los datos se conservan en los volúmenes)
docker compose down -v                  # parar y BORRAR todos los datos
```

La primera vez tarda un poco (descarga `python:3.13-slim` y `postgres:17-alpine`).
Comprobación rápida:

```bash
curl -s localhost:8080/health
python3 scripts/p16.py status
```

## 3. Usar el almacén con curl

Las claves pueden llevar `/` (como carpetas). `/objects/...` usa el bucket `default`;
para otros buckets: `/buckets/<bucket>/objects/<clave>` (se crean solos al primer PUT).

**Subir (PUT)**

```bash
curl -X PUT --data-binary @foto.jpg http://localhost:8080/objects/fotos/foto.jpg
```

La respuesta indica el id estable, la versión, el SHA-256 y en qué nodos quedaron las copias:

```json
{
  "id": "01M3MR91WWM02M7GK81PQSDTKY",
  "key": "fotos/foto.jpg",
  "version": 1,
  "checksum_sha256": "9117d4…",
  "replicas": ["storage3", "storage1"],
  "durability": "2 copias en nodos distintos, cada una con checksum verificado y fsync; …"
}
```

Opciones útiles del PUT:

```bash
# tipo de contenido y metadatos de usuario
curl -X PUT -H "Content-Type: image/jpeg" -H "X-Meta-Autor: ana" \
     --data-binary @foto.jpg localhost:8080/objects/fotos/foto.jpg
# crear sólo si no existe
curl -X PUT -H "If-None-Match: *" --data-binary @foto.jpg localhost:8080/objects/fotos/foto.jpg
# en otro bucket
curl -X PUT --data-binary @notas.txt localhost:8080/buckets/trabajo/objects/notas.txt
```

**Descargar (GET)**

```bash
curl -o copia.jpg http://localhost:8080/objects/fotos/foto.jpg
curl -si http://localhost:8080/objects/fotos/foto.jpg -o /dev/null   # ver cabeceras
```

Cabeceras interesantes: `X-Served-By` (nodo que sirvió el objeto), `X-Read-Attempts`
(p. ej. `storage2=error,storage1=ok`), `ETag` (= SHA-256), `X-Object-Version`.

**Metadatos (HEAD)**

```bash
curl -I http://localhost:8080/objects/fotos/foto.jpg        # incluye X-Replicas
```

**Listar (LIST)**

```bash
curl http://localhost:8080/objects                           # bucket default
curl "http://localhost:8080/objects?prefix=fotos/&limit=50"  # por prefijo, paginado (after=)
curl http://localhost:8080/buckets                           # buckets existentes
```

**Borrar (DELETE)**

```bash
curl -X DELETE http://localhost:8080/objects/fotos/foto.jpg  # 204 (idempotente)
```

Documentación interactiva (Swagger) en <http://localhost:8080/docs>.

## 4. CLI de ayuda: `scripts/p16.py`

Sólo usa la librería estándar de Python. Resume lo más útil:

```bash
python3 scripts/p16.py status                 # nodos, salud, reparto de réplicas, estado de los objetos
python3 scripts/p16.py watch                  # abre la interfaz visual en el navegador (ver § 4.1)
python3 scripts/p16.py watch --terminal       # estado + eventos en la terminal, refrescado cada 2 s
python3 scripts/p16.py put foto.jpg fotos/foto.jpg
python3 scripts/p16.py get fotos/foto.jpg copia.jpg   # indica qué nodo lo sirvió
python3 scripts/p16.py head fotos/foto.jpg
python3 scripts/p16.py ls fotos/
python3 scripts/p16.py rm fotos/foto.jpg
python3 scripts/p16.py seed 20                # sube 20 objetos de prueba (demo/…)
python3 scripts/p16.py locate fotos/foto.jpg  # ranking HRW (dónde debería estar) vs réplicas reales
python3 scripts/p16.py events 30              # fallos, reparaciones, corrupción, GC…
python3 scripts/p16.py wait converged         # esperar a que todo esté en su sitio
python3 scripts/p16.py verify foto.jpg fotos/foto.jpg  # ¿lo descargado es idéntico a mi fichero?
```

Para buckets distintos de `default`: `python3 scripts/p16.py --bucket trabajo ls`.
La lista completa, con ejemplos, está en [`docs/COMANDOS.md`](docs/COMANDOS.md).

### 4.1 Interfaz visual en vivo

```bash
python3 scripts/p16.py watch
```

Abre en el navegador <http://localhost:8080/ui>, una vista del almacén que se mueve con cada petición:

- **Plano de aplicación**: el cliente, el gateway (con sus módulos: API, ubicación HRW, detector de
  fallos, reconciliador, scrub) y PostgreSQL. Cada `curl` aparece en la lista del cliente y se
  anima su recorrido real: `PUT` → SHA-256 y HRW → 2 copias (`✓ guardado (fsync)`) → `commit` en
  PostgreSQL → `201`; `GET` → "¿dónde está?" → lectura de una réplica (`✗ SHA-256 no coincide` si
  está corrupta) → `200`; `HEAD`/`LIST` sólo consultan metadatos; `DELETE` deja una lápida.
- **Plano de datos**: cada Storage Node es una estantería y cada caja una **réplica**; **mismo
  color = mismo objeto**, así se ve que cada objeto está en 2 nodos distintos. Estados: `↗` copia
  fuera de su sitio, hueco punteado = "aquí debería haber una copia", rojo rayado = corrupta, gris
  tachada = pendiente de borrado físico. Un nodo caído se tapa con una trama roja.
- **Eventos**: sondeos del detector de fallos, caídas, re-replicación y reequilibrado (paquetes que
  viajan de nodo a nodo), corrupción, GC…
- Pulsa una caja para ver el **ranking HRW** de ese objeto y dónde están sus réplicas (Esc para soltar).
  Arriba puedes poner la animación en modo **lento** para explicarla en la defensa.

Déjala a la vista (idealmente proyectada) y lanza los `curl` o `p16.py` en una terminal.

### Guiones: varias acciones desde un fichero

`run` ejecuta en orden las acciones escritas en un fichero (una por línea) y puede recibir
tus propios ficheros como argumentos:

```bash
python3 scripts/p16.py run escenarios/archivo.txt ~/Desktop/foto.jpg     # ciclo completo de tu fichero
python3 scripts/p16.py run escenarios/caida-nodo.txt ~/Desktop/foto.jpg  # tu fichero con un nodo caído
python3 scripts/p16.py run escenarios/membresia.txt                      # añadir y retirar storage4
python3 scripts/p16.py run escenarios/demo-completa.txt                  # demo completa con pausas
```

Formato y cómo escribir los tuyos: [`docs/COMANDOS.md` § 3](docs/COMANDOS.md#3-guiones-ejecutar-varias-acciones-desde-un-fichero).

## 5. Probar la tolerancia a fallos

Deja abierta la interfaz visual (`python3 scripts/p16.py watch`) para ver los cambios en vivo.

| Escenario | Comandos | Qué debe pasar |
|---|---|---|
| Nodo caído | `docker compose stop storage2` y luego `python3 scripts/p16.py get demo/objeto-001.txt` | GET sigue funcionando (desde la otra réplica); storage2 pasa a `SUSPECT` y `DOWN` (~6 s); los PUT van a los otros nodos |
| Re-replicación | esperar 30 s con el nodo caído (`python3 scripts/p16.py wait repaired`) | Las copias perdidas se re-crean en los nodos vivos |
| Vuelta del nodo | `docker compose start storage2` | `RECOVERING → HEALTHY`; recibe sus réplicas, se retiran las sobrantes, se aplican los borrados pendientes |
| Nodo colgado | `docker compose pause storage3` / `unpause` | Timeouts → `SUSPECT`/`DOWN`; el servicio sigue |
| Dos nodos caídos | `docker compose stop storage1 storage2` | PUT → 503 (no hay 2 nodos); GET → 200 si hay copia en storage3, si no 503 (nunca datos erróneos) |
| Corrupción | `python3 scripts/p16.py corrupt demo/objeto-003.txt` y después `get` | `intentos: storageX=corrupt,storageY=ok`; la copia mala se repara sola |
| Corrupción sin lecturas | `corrupt …` y después `python3 scripts/p16.py scrub` | El scrub la detecta y la repara |
| Disco perdido | `python3 scripts/p16.py wipe storage1` y `python3 scripts/p16.py reconcile` | Réplicas `MISSING` detectadas y repuestas |
| Blob huérfano | `python3 scripts/p16.py orphan storage2` | Se elimina tras 60 s (`gc.orphan`) |
| PostgreSQL caído | `docker compose stop metadata-db` / `start` | 503 mientras no está; se recupera solo |

Ver los ficheros reales de un nodo: `docker compose exec storage1 ls -l /data/blobs`.

## 6. Añadir, retirar y ponderar nodos

```bash
# añadir storage4 (se registra solo y recibe ~25 % de las réplicas)
docker compose --profile scale up -d --wait storage4
python3 scripts/p16.py wait converged && python3 scripts/p16.py status

# retirarlo de forma ordenada (sus datos pasan a los demás)
python3 scripts/p16.py drain storage4
python3 scripts/p16.py wait storage4 DECOMMISSIONED
docker compose --profile scale stop storage4

# dar más capacidad a un nodo (recibe proporcionalmente más datos)
python3 scripts/p16.py weight storage3 2
```

Las mismas operaciones por HTTP: `POST /cluster/nodes/{id}/drain`, `POST /cluster/nodes/{id}/activate`,
`PATCH /cluster/nodes/{id}` con `{"weight": 2}`.

## 7. Demo guiada y tests

```bash
FAST=1 ./scripts/demo.sh           # demo completa paso a paso (ENTER para avanzar)
```

```bash
# tests end-to-end contra el clúster real (≈1–2 min con tiempos cortos)
REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3 docker compose up -d --build --wait
python3 tests/e2e_test.py

# tests unitarios: ubicación/ids y convergencia del reconciliador con dobles en memoria
docker compose exec gateway python -m unittest discover -s tests -t .
# (o en local, sin Docker: cd gateway && pip install -r requirements.txt && python3 -m unittest discover -s tests -t .)

# comparar hash % N, anillo consistente y HRW al pasar de 3 a 4 nodos
docker compose exec gateway python -m tools.simulate_placement
```

## 8. Configuración

Variables del gateway (se pueden pasar al lanzar `docker compose up`, p. ej.
`REPAIR_DELAY_SECONDS=10 docker compose up -d`):

| Variable | Defecto | Significado |
|---|---|---|
| `REPLICATION_FACTOR` | 2 | Copias por objeto (RF) |
| `WRITE_ACKS` | 2 | Copias confirmadas necesarias para responder a un PUT (W ≤ RF) |
| `MAX_OBJECT_SIZE_MB` | 64 | Tamaño máximo de objeto |
| `STORAGE_TIMEOUT_SECONDS` | 5 | Timeout de cada operación contra un nodo |
| `HEALTH_INTERVAL_SECONDS` / `HEALTH_TIMEOUT_SECONDS` | 2 / 1 | Sondeo del detector de fallos |
| `SUSPECT_AFTER` / `DOWN_AFTER` | 1 / 3 | Fallos seguidos para SUSPECT / DOWN |
| `REPAIR_DELAY_SECONDS` | 30 | Tiempo en DOWN antes de re-replicar sus datos |
| `RECONCILE_INTERVAL_SECONDS` | 10 | Periodo de la reconciliación |
| `SCRUB_INTERVAL_SECONDS` | 300 | Periodo del scrub (verificación de checksums) |
| `ORPHAN_GRACE_SECONDS` | 60 | Edad mínima de un blob sin metadatos antes de borrarlo |
| `DELETE_GRACE_SECONDS` | 5 | Retardo del borrado físico (protege lecturas en curso) |
| `MAX_COPIES_PER_CYCLE` | 200 | Límite de copias por ciclo (reequilibrado gradual) |

## 9. Estructura del proyecto

```
docker-compose.yml         gateway, storage1-3, storage4 (perfil scale), metadata-db
gateway/app/
  main.py                  arranque, tareas de fondo, errores
  api_objects.py           API de objetos (PUT/GET/HEAD/DELETE/LIST)
  api_cluster.py           API de operación (/cluster/...)
  objects_service.py       semántica de cada operación (plano de aplicación)
  placement.py             rendezvous hashing ponderado (HRW)
  membership.py            membresía y máquina de estados de salud
  health_monitor.py        detector de fallos
  reconciler.py            reconciliación, reparación, reequilibrado, GC, scrub
  metadata.py              todas las consultas SQL (fuente de verdad)
  db.py                    esquema de PostgreSQL
  storage_client.py        cliente HTTP hacia los nodos
  live.py                  feed en memoria de la actividad (para la interfaz visual)
  static/ui.html           interfaz visual en vivo (/ui)
gateway/tests/             tests unitarios (placement, ids)
gateway/tools/             simulación comparativa de estrategias de ubicación
storage-node/app/
  main.py                  API del nodo (blobs, health, push, join)
  blob_store.py            escritura atómica con fsync y verificación SHA-256
scripts/p16.py             CLI de ayuda (incluye `run` para guiones)
scripts/demo.sh            demo guiada
escenarios/                guiones de acciones listos para `p16.py run`
tests/e2e_test.py          pruebas end-to-end (21 comprobaciones)
docs/DISENO.md             diseño y justificación (A–H)
docs/DEMO.md               guion de la defensa
docs/COMANDOS.md           referencia de todos los comandos
```

## 10. Problemas frecuentes

- **`port is already allocated`**: el 8080 está ocupado; libéralo o cambia `"8080:8080"` en
  `docker-compose.yml` (y usa `P16_URL=http://localhost:NUEVO` con la CLI).
- **La CLI dice que no puede conectar**: `docker compose ps` y `docker compose logs gateway`.
- **Empezar de cero**: `docker compose --profile scale down -v`.
- **Ver qué está pasando**: `docker compose logs -f gateway` o `python3 scripts/p16.py events 50`.

---

## Enunciado inicial del proyecto

# 🥈 P16 — Almacenamiento distribuido de objetos

Una arquitectura posible:

```text
             ┌──────────┐
             │ Cliente  │
             └────┬─────┘
                  │
             ┌────▼─────┐
             │  Server  │
             └────┬─────┘
                  │
        ┌─────────┼─────────┐
        │         │         │
   ┌────▼───┐ ┌───▼────┐ ┌──▼─────┐
   │ Nodo 1 │ │ Nodo 2 │ │ Nodo 3 │
   └────────┘ └────────┘ └────────┘
```

Metodos a implementar primero:

```text
PUT
GET
DELETE
LIST
```

Propiedades a cumplir:

- Replicación.
- Hashing/ubicación.
- Checksums.
- Fallo de nodos.
- Recuperación.
- Reconciliación.

Esto permite empezar con algo sencillo y añadir progresivamente los conceptos de distribución.

---
