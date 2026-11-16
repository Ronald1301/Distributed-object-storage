# P16 — Diseño del servicio distribuido de almacenamiento de objetos

Este documento justifica cada decisión arquitectónica. Sigue el orden pedido en el
enunciado: **A** arquitectura · **B** decisiones distribuidas · **C** modelo de datos ·
**D** API · **E** flujos · **F** fallos · **G** Docker · **H** demo. Al final se
incluyen limitaciones y la comparación con S3, Ceph y MinIO.

> Stack: Python 3.13 · FastAPI · asyncpg · httpx · PostgreSQL 17 · Docker Compose.
> Sin Kafka, Redis, Consul ni consenso propio: todo lo distribuido está en el código del proyecto.

---

## Resumen de decisiones

| Problema | Decisión | Por qué (en una línea) |
|---|---|---|
| Identificador | **ULID** por objeto (estable) + ULID por versión (blob) | Único sin coordinación, ordenable, independiente de dónde esté el dato |
| Ubicación | **Rendezvous hashing ponderado (HRW)** + tabla de réplicas reales | Reequilibrado mínimo (sólo 1/N se mueve), soporta pesos, trivial de explicar |
| Redundancia | **Replicación completa, RF = 2**, réplicas simétricas (sin primario) | Tolera la pérdida de 1 nodo; blobs inmutables ⇒ no hace falta primario que ordene |
| Durabilidad | **W = 2**: 2xx sólo tras 2 `fsync` verificados + commit en PostgreSQL | Garantía clara: un objeto confirmado sobrevive a la pérdida total de un nodo |
| Consistencia | Metadatos como **fuente de verdad**; commit = **punto de linealización** | Lectura-tras-escritura fuerte por clave sin quórums de lectura |
| Integridad | **SHA-256 extremo a extremo** (escritura, lectura, copia y *scrub*) | Detecta corrupción silenciosa y nunca la propaga |
| Metadatos | **PostgreSQL** centralizado (como el *master* de GFS/HDFS) | Transacciones ACID: sin consenso propio |
| Detección de fallos | Sondeo `/health` + timeouts + detección pasiva; estados **HEALTHY / SUSPECT / DOWN / RECOVERING** | Suficiente con un único observador; sin falsos positivos por un solo fallo |
| Reparación | **Bucle de reconciliación** (anti-entropía) + *read-repair* + *scrub* | Un único mecanismo "estado real → estado deseado" resuelve todo |
| Membresía | *Join* automático, *drain*, cambio de peso | Añadir/quitar nodos mueve sólo lo necesario, con throttling |

---

## A. Arquitectura

```
                               ┌────────────┐
                               │  Cliente   │  curl / scripts/p16.py
                               └─────┬──────┘
                                     │ HTTP/REST (único puerto expuesto: 8080)
             ┌───────────────────────▼────────────────────────────┐
             │                     GATEWAY                         │
             │  ┌──────────────────────┐  ┌─────────────────────┐  │
             │  │ Plano de aplicación  │  │  Coordinador del    │  │
             │  │ PUT GET HEAD DELETE  │  │  plano de datos     │  │
             │  │ LIST  (api_objects)  │  │  placement (HRW)    │  │
             │  └──────────┬───────────┘  │  health monitor     │  │
             │             │              │  reconciler / scrub │  │
             │             │              └─────────┬───────────┘  │
             └─────────────┼────────────────────────┼──────────────┘
                   SQL     │                        │  HTTP (red interna)
          ┌────────────────▼───┐        ┌───────────┼───────────────┐
          │    PostgreSQL      │        ▼           ▼               ▼
          │    (metadatos)     │   ┌─────────┐ ┌─────────┐    ┌─────────┐
          │ objects            │   │storage1 │ │storage2 │ …  │storage4 │ (opcional)
          │ object_replicas    │   │ /blobs  │◀┼─push────┼───▶│ /blobs  │ copia nodo→nodo
          │ nodes, deletions   │   └────┬────┘ └────┬────┘    └────┬────┘
          └────────────────────┘        ▼           ▼              ▼
                                   storage1-data storage2-data  storage4-data  (volúmenes propios)
```

### Componentes

| Componente | Responsabilidad | Estado que guarda |
|---|---|---|
| **Gateway** (`gateway/`) | Punto de entrada; API de objetos; calcula ubicación; escribe/lee réplicas; detector de fallos; reconciliación, reparación, reequilibrado, GC y scrub | Sólo estado de salud en memoria (persistido en `nodes`). **No guarda bytes** |
| **Storage Node** ×3 (+1) (`storage-node/`) | Guarda **blobs inmutables** identificados por ULID en su volumen; verifica checksum al escribir; recalcula checksum (scrub); copia blobs a otros nodos | Ficheros en su volumen Docker |
| **PostgreSQL** (`metadata-db`) | Fuente de verdad de los metadatos: objetos, versiones, ubicación real de réplicas, membresía, cola de borrados, eventos | Tablas (volumen `metadata-data`) |

Los Storage Nodes **no saben nada** de buckets, claves, réplicas ni de los otros nodos
(salvo cuando el gateway les pide copiar un blob a una URL). Toda la lógica distribuida
está en un único sitio (el gateway), lo que hace el sistema explicable.

### ¿Es adecuada la separación plano de aplicación / plano de datos?

Sí, y está materializada en el código:

- **Contrato de aplicación** (`api_objects.py`, `objects_service.py`): el cliente sólo ve
  `(bucket, key) → bytes + metadatos`, con ETag, versión y metadatos de usuario. No sabe
  cuántos nodos hay ni dónde está cada copia.
- **Plano de datos distribuido** (`placement.py`, `storage_client.py`, `health_monitor.py`,
  `reconciler.py`, `storage-node/`): ubicación, réplicas, detección de fallos, integridad,
  reparación.
- **Plano de control/metadatos** (PostgreSQL + `metadata.py`): la "verdad" sobre qué existe
  y dónde. Es el que conecta ambos planos.

Matiz honesto para la defensa: el gateway aloja la API **y** el coordinador del plano de
datos en el mismo proceso. Es una decisión de simplicidad (igual que el *master* de
GFS/HDFS coordina el plano de datos). Como el gateway **no guarda estado propio** (todo está
en PostgreSQL), podría separarse en dos servicios (API sin estado + controlador de
reparación) sin cambiar el diseño.

---

## B. Decisiones distribuidas

### B.1 Identificadores estables

| Opción | Ventajas | Problemas |
|---|---|---|
| UUIDv4 | Único sin coordinación | No ordenable; nada más |
| **ULID** | Único sin coordinación, **ordenable por tiempo**, 26 caracteres legibles | — |
| Hash del contenido | Deduplicación, integridad implícita | El id **cambia** al sobrescribir ⇒ no es estable para el objeto |
| `bucket + key` | Legible | Es un **nombre**, no una identidad: renombrar/borrar y recrear lo reutiliza |

Decisión (dos niveles, como S3/Ceph internamente):

1. **`objects.id` (ULID)**: identidad estable del objeto lógico. Se crea en el primer PUT
   de un nombre y **no cambia** al sobrescribir ni al mover réplicas. Es la entrada de HRW,
   así que todas las versiones de un objeto viven en los mismos nodos deseados.
2. **`blob_id` (ULID)**: cada versión escrita es un **blob nuevo e inmutable**. Nunca se
   modifica un fichero en sitio ⇒ no hay escrituras parciales visibles ni conflictos entre
   réplicas de "la misma" versión.
3. `(bucket, key)` es el **nombre**; se traduce a `id` en los metadatos (índice único parcial
   sobre objetos vivos). Tras un DELETE, recrear el mismo nombre genera un id nuevo.

El checksum SHA-256 cumple el papel del "hash de contenido" (integridad y ETag) sin usarse
como identidad.

### B.2 Ubicación (placement)

**Alternativas analizadas** (medidas con `tools/simulate_placement.py`, 20 000 objetos,
RF=2, pasando de 3 a 4 nodos; el óptimo teórico es mover el 25 % de las réplicas):

| Estrategia | Réplicas movidas | Movidas entre nodos **antiguos** (tráfico inútil) | Reparto con 4 nodos |
|---|---|---|---|
| `hash(id) % N` | **49,6 %** | **25,1 %** | 24,5–25,5 % |
| Anillo consistente, 1 nodo virtual | 20,3 % | 0 % | **20,3–29,7 %** (desequilibrado) |
| Anillo consistente, 100 nodos virtuales | 28,2 % | 0 % | 22,1–28,2 % |
| **Rendezvous / HRW (elegida)** | **24,9 %** | **0 %** | **24,8–25,3 %** |
| Tabla de asignación explícita | lo que decida el admin | — | requiere mantener la tabla para cada objeto |

**Decisión: Weighted Rendezvous Hashing (HRW)**, el mismo principio que el `straw2` del
algoritmo CRUSH de Ceph:

```
para cada nodo n activo:  u = hash(object_id, n) ∈ (0,1) ;  score(n) = −peso(n) / ln(u)
ordenar nodos por score descendente  →  lista de preferencia del objeto
ubicación deseada = los RF primeros
```

- Es **determinista**: cualquiera que conozca la lista de miembros calcula lo mismo.
- Las RF réplicas quedan **siempre en nodos distintos** (son los RF primeros de un ranking).
- Los siguientes de la lista son los **sustitutos naturales** cuando uno falla.
- Con pesos, la probabilidad de ser elegido es proporcional a la capacidad (test incluido).
- Coste O(N) por objeto: irrelevante para decenas de nodos.

**Decisión complementaria (híbrida):** HRW decide dónde *deberían* estar las réplicas; la
tabla `object_replicas` registra dónde *están realmente*. Las lecturas usan la tabla, no el
hash. Así la corrección nunca depende de que la ubicación deseada esté al día: si un nodo
estaba caído durante un PUT, la copia se escribe en el siguiente de la lista y se anota;
la reconciliación la moverá más tarde. Es la combinación que usan GFS/HDFS (tabla) y
Dynamo/Ceph (función), tomando lo mejor de cada una.

**¿Qué pasa al pasar de 3 → 4 nodos?**

- Con `hash % N` cambia el divisor y casi todo se re-mapea: se mueve ~50 % de las réplicas
  y la mitad de ese tráfico es entre nodos antiguos (inútil).
- Con HRW sólo cambian los objetos para los que el nodo nuevo entra en su top-2: se mueve
  ~1/4 de las réplicas y **todas van hacia el nodo nuevo** (probado en
  `test_adding_node_moves_minimum`).
- **Reequilibrado**: no hay un proceso especial. El nodo nuevo hace *join*, cambia la
  ubicación deseada de ~50 % de los objetos, y el bucle de reconciliación copia
  (nodo→nodo) las réplicas que faltan en storage4 y, **sólo cuando la copia nueva está
  confirmada**, retira la sobrante. Se limita a `MAX_COPIES_PER_CYCLE` copias por ciclo para
  no saturar la red (como `osd_max_backfills` en Ceph).

### B.3 Replicación

- **RF = 2** (configurable). Réplicas **simétricas, sin primario**.
  - Primario + réplica sirve cuando las réplicas aplican *mutaciones* y alguien debe fijar
    su orden. Aquí los blobs son **inmutables** y el orden de versiones lo fija el commit en
    PostgreSQL, así que un primario no aporta nada y añadiría un caso de fallo (caída del
    primario ⇒ elección). El primero de la lista HRW se usa sólo como réplica **preferida
    para leer** (determinista, útil en la demo), sin autoridad especial.
- **Cómo se eligen los nodos**: los RF primeros *escribibles* de la lista de preferencia HRW.
- **Nunca dos copias en el mismo nodo**: la lista de preferencia no repite nodos y la PK
  `(object_id, node_id)` de `object_replicas` lo impide también en los metadatos.
- **Cómo se actualizan**: una sobrescritura es un blob nuevo escrito en paralelo a RF
  nodos; el commit cambia atómicamente el puntero `objects.blob_id`; los blobs antiguos van a
  la cola de borrado. No hay "actualización en sitio".
- **Si una réplica falla durante el PUT**: se sustituye por el siguiente nodo de la lista
  (*sloppy quorum*, como Dynamo). Si no se consiguen W copias, el PUT falla y no se publica nada.
- **Si una réplica falla después**: ver B.9 (reparación).

### B.4 Durabilidad: ¿cuándo termina bien un PUT?

| Política | Latencia | Garantía | Problema |
|---|---|---|---|
| ACK de 1 copia (replicación asíncrona) | mínima | se puede perder un objeto ya confirmado si ese nodo muere antes de replicar | promesa débil |
| **ACK de 2 copias = RF (síncrona)** | la del nodo más lento | un objeto confirmado sobrevive a la pérdida **permanente** de 1 nodo | con 1 solo nodo vivo no se puede escribir |
| Quórum W de N (p. ej. 2 de 3) | intermedia | idem con N réplicas | necesita R+W>N o metadatos de versión para leer |

**Decisión: W = RF = 2.** Garantía declarada (también en `GET /cluster/config`):

> Un PUT responde `201`/`200` **sólo después** de que (1) el blob está en disco en **2 nodos
> distintos**, cada uno habiendo verificado el SHA-256 recibido, hecho `fsync` del fichero y
> `rename` atómico + `fsync` del directorio, y (2) el commit de metadatos en PostgreSQL ha
> terminado. Ningún objeto confirmado se pierde mientras sobreviva al menos 1 de sus 2 nodos.

`WRITE_ACKS=1` está soportado para experimentar el compromiso: el PUT se confirma con una
copia y la reconciliación crea la segunda después (más disponibilidad, menos durabilidad).

No necesitamos quórum de lectura: como la versión (blob + checksum) sale de los metadatos,
**cualquier** réplica que pase el SHA-256 es la correcta (R = 1).

### B.5 Consistencia

Principio: **los metadatos confirmados en PostgreSQL definen la verdad**; los blobs son
inmutables y verificables. Resultado por operación:

| Operación | Garantía | Coste asumido |
|---|---|---|
| PUT | Atómico: o se ve la versión completa nueva o la anterior; nunca un objeto a medias | Latencia de 2 escrituras síncronas + 1 transacción |
| GET | **Lectura-tras-escritura fuerte**: tras un PUT 2xx, cualquier GET ve esa versión o una posterior; nunca datos obsoletos ni corruptos (en el peor caso 503) | Una consulta a PostgreSQL por lectura |
| HEAD | Igual que GET, sólo metadatos (no contacta con los nodos) | — |
| DELETE | Tras 204, GET/HEAD/LIST dan 404 inmediatamente; el espacio se libera **eventualmente** | Borrado físico asíncrono |
| LIST | Consistente con los metadatos confirmados (no ve PUT en curso ni borrados) | Depende sólo de PostgreSQL |
| PUT concurrentes al mismo nombre | *Last-writer-wins* según el orden de commit; opcional control optimista con `If-Match` / `If-None-Match: *` (412 si falla) | — |

**Orden de las operaciones del PUT.** Se analizaron las dos opciones del enunciado:

- *metadatos → réplicas*: si se cae tras el commit, hay metadatos que apuntan a datos
  inexistentes ⇒ lecturas que fallan. **Descartado.**
- *réplicas → metadatos*: si se cae antes del commit sólo quedan blobs huérfanos,
  **invisibles** (nadie los referencia) y recolectables. **Elegido**, con un refinamiento:

```
1. reservar id (INSERT … status='PENDING' si el nombre es nuevo)   ← sólo fija la identidad
2. escribir blob nuevo en RF nodos (fsync, checksum)                ← invisible
3. COMMIT: status='ACTIVE', version+1, blob_id=nuevo, réplicas      ← PUNTO DE LINEALIZACIÓN
4. responder 2xx
```

La reserva (paso 1) existe para que el id —y por tanto la ubicación HRW— se conozca antes
de escribir datos. Una reserva `PENDING` nunca es visible y se limpia si no se confirma.

**CAP**: ante una partición entre el gateway y PostgreSQL el sistema elige **consistencia**
(responde 503 en vez de servir datos sin verificar). Ante la caída de nodos de
almacenamiento sigue disponible mientras quede una réplica válida (lecturas) o W nodos
(escrituras).

### B.6 Integridad

SHA-256 del objeto completo, verificado en **cuatro** puntos:

1. **Escritura**: el gateway calcula el SHA-256 y lo envía en `X-Checksum-SHA256`; el nodo
   lo recalcula mientras escribe y rechaza (`422`) si no coincide ⇒ detecta corrupción en tránsito.
2. **Lectura**: el gateway recalcula el SHA-256 de los bytes recibidos y lo compara con
   `objects.checksum`. Si difiere: marca la réplica `CORRUPT`, prueba la siguiente, sirve la
   válida y dispara la reparación (*read-repair*). El cliente nunca recibe bytes corruptos.
3. **Copia nodo→nodo** (reparación/reequilibrado): el origen verifica su copia antes de
   enviarla (`409` si está corrupta) y el destino vuelve a verificarla ⇒ la corrupción
   **nunca se propaga**.
4. **Scrub profundo** periódico (`SCRUB_INTERVAL_SECONDS`, o `POST /cluster/scrub`): cada
   nodo recalcula el SHA-256 de sus blobs **en local** (no viajan los datos) ⇒ detecta
   corrupción de objetos que nadie lee (como el *deep scrub* de Ceph).

¿Es adecuada para P16? Sí: detecta la corrupción silenciosa extremo a extremo y la repara
con otra réplica. Limitaciones conocidas: el checksum es por objeto completo (para objetos
enormes convendría por bloques, como HDFS o el *multipart* de S3) y el gateway almacena el
objeto en memoria para poder verificarlo antes de enviarlo (límite `MAX_OBJECT_SIZE_MB`).

### B.7 Metadatos

- **Propiedad**: los metadatos son **propiedad exclusiva del gateway** (único escritor) y
  residen en PostgreSQL. Los nodos sólo guardan un *sidecar* informativo
  (`<blob>.meta.json`: checksum, tamaño, object_id), útil para diagnóstico o una hipotética
  reconstrucción ante desastre, pero **no** es fuente de verdad.
- **Consistencia de los metadatos**: toda modificación que toca varias filas es una
  transacción que bloquea la fila del objeto (`SELECT … FOR UPDATE`): commit de versión,
  borrado, alta/baja de réplica. Las condiciones se re-comprueban dentro de la transacción
  (p. ej. "no retirar una réplica si quedarían menos de RF válidas").
- **Relación metadatos ↔ datos** (invariantes):
  - Un objeto existe ⇔ fila en `objects` con `status='ACTIVE'`.
  - Un blob en un nodo es válido ⇔ fila `(object_id, node_id)` en `object_replicas` con ese
    `blob_id` y `status='OK'`.
  - Cualquier otro blob en disco es **basura** y se recolecta (tras un periodo de gracia).
- **PostgreSQL disponible, nodo caído**: los metadatos siguen indicando dónde está cada
  copia; el gateway lee de otra réplica y la reconciliación re-replica si la caída se
  prolonga. `HEAD` y `LIST` no se ven afectados.
- **Objeto en disco sin metadatos** (PUT que murió antes del commit, versión antigua,
  borrado mientras el nodo estaba caído): es un **huérfano** por definición ⇒ la fase de
  inventario lo borra cuando supera `ORPHAN_GRACE_SECONDS` (la gracia evita borrar un PUT en
  curso). La edad la calcula el propio nodo: no dependemos de relojes sincronizados.
- **Metadatos sin objeto en disco** (disco sustituido, fichero borrado): el inventario lo
  detecta y marca la réplica `MISSING`; la reconciliación la repone desde otra copia. Si no
  quedase ninguna copia válida, el objeto se reporta como `unavailable` (GET → 503, nunca
  datos erróneos).
- **PostgreSQL caído**: el gateway responde 503 (no puede garantizar el contrato). Es el
  punto único de fallo asumido (ver Limitaciones).

### B.8 Detección de fallos

Se usa lo mínimo necesario, **sin consenso** (hay un único observador y una única fuente de
verdad, así que no hay nada que acordar entre pares):

| Mecanismo | Implementación | Para qué |
|---|---|---|
| Health checks (pull) | El gateway sondea `GET /health` de cada nodo cada 2 s (timeout 1 s) y verifica su identidad | Detector principal |
| Timeouts | Toda operación del plano de datos tiene timeout (5 s) | No quedarse colgado ante un nodo congelado |
| Detección pasiva | Un error de red en un GET/PUT real marca el nodo `SUSPECT` al instante | Reaccionar antes del siguiente sondeo |
| *Join* (push) | Cada nodo se registra al arrancar (`POST /cluster/nodes/join`) | Membresía dinámica |
| Healthchecks de Docker | `docker compose ps` muestra el estado | Operación |

Máquina de estados de salud (`membership.py`):

```
          1 fallo                 3 fallos seguidos
HEALTHY ──────────▶ SUSPECT ─────────────────────▶ DOWN
   ▲  ◀── responde ───┘                             │
   │                                                │ responde
   └──── reconciliación completada ◀── RECOVERING ◀─┘
```

- `SUSPECT`: no recibe escrituras nuevas; se lee de él sólo como último recurso. Evita
  declarar muerto un nodo por un único paquete perdido.
- `DOWN`: excluido de lecturas y escrituras. Sus réplicas **siguen contando** para RF
  durante `REPAIR_DELAY_SECONDS` (30 s) por si es un reinicio; después se re-replican. Es la
  distinción *down* vs *out* de Ceph (`mon_osd_down_out_interval`): evita mover datos por
  fallos transitorios.
- `RECOVERING`: ha vuelto, pero puede haberse perdido borrados/escrituras; dispara una
  reconciliación inmediata y pasa a `HEALTHY` al terminarla.

La salud **no** cambia la ubicación deseada (sólo la membresía lo hace): un fallo
transitorio no debe provocar un reequilibrado.

### B.9 Reparación y reconciliación

Un único **bucle de control** (`reconciler.py`, cada `RECONCILE_INTERVAL_SECONDS`, y también
bajo demanda) hace converger el estado real al deseado:

| Fase | Qué hace | Resuelve |
|---|---|---|
| 1. Inventario | Pide a cada nodo accesible la lista de sus blobs y la compara con `object_replicas` | Réplicas ausentes (`MISSING`), huérfanos (GC) |
| 2. Convergencia por objeto | Para cada objeto: calcula su ubicación deseada; copia (nodo→nodo, verificado) a los nodos deseados que no tengan copia válida; si un deseado lleva caído más de `REPAIR_DELAY`, usa el siguiente de la lista; **sólo después** retira las copias sobrantes | Reparación, re-replicación, reequilibrado, drenado |
| 3. Cola de borrados | Borra físicamente los blobs pendientes en nodos accesibles (reintentos) | DELETE, versiones sobrescritas, sobrantes |
| 4. Reservas caducadas | Elimina reservas `PENDING` de PUT que nunca confirmaron | Metadatos inconsistentes |
| 5. Membresía | `RECOVERING → HEALTHY`; `DRAINING` vacío `→ DECOMMISSIONED` | Cambios de membresía |

Además:

- **Read-repair**: un GET que encuentra una copia corrupta o ausente encola la reparación
  inmediata de ese objeto.
- **Scrub**: verificación periódica de checksums (B.6), con reparación inmediata.
- **Seguridad de la reparación**: nunca se retira una copia hasta que la ubicación deseada
  tiene RF copias válidas (comprobado dentro de la transacción); las copias se registran sólo
  si el objeto sigue en la misma versión (si no, quedan huérfanas y se recolectan).
- **Exclusión mutua**: un `asyncio.Lock` en el proceso y un *advisory lock* de PostgreSQL
  entre procesos: si algún día hubiera dos gateways, sólo uno repararía a la vez, sin
  implementar consenso.

### B.10 Reequilibrado y cambios de membresía/capacidad

- **Alta** (`docker compose --profile scale up -d storage4`): el nodo hace *join*, entra en
  la ubicación deseada y la reconciliación le traslada su parte.
- **Baja ordenada** (`POST /cluster/nodes/{id}/drain`): el nodo sale de la ubicación deseada
  (`DRAINING`), sus copias se replican en otros nodos y, cuando no le queda ninguna, pasa a
  `DECOMMISSIONED` y puede apagarse. Se rechaza si quedarían menos nodos activos que RF.
- **Capacidad** (`PATCH /cluster/nodes/{id}` `{"weight": 2}`): HRW ponderado reparte en
  proporción al peso.

¿Es realista el "25 % / 25 % / 25 % / 25 %" del enunciado? Sí en *esperanza*, no exactamente:
HRW da un reparto estadístico (con 20 000 objetos, 24,8–25,3 %; con 50 objetos la varianza
es visible). Con RF=2 y pesos distintos hay además un tope: un nodo no puede tener más del
50 % de las réplicas (nunca guarda dos copias del mismo objeto). La implementación mínima
que cumple el objetivo académico es exactamente la adoptada: función de ubicación con
movimiento mínimo + un bucle que migra lo desubicado con límite de velocidad.

---

## C. Modelo de datos

```
nodes (1) ────────────────< object_replicas >──────────────── (1) objects
node_id PK                   object_id FK ─┐                    id PK (ULID)
url, weight                  node_id  FK   │ PK compuesta       bucket, object_key  (único si vivo)
membership                   blob_id       │                    status PENDING|ACTIVE|DELETED
  ACTIVE|DRAINING|           status OK|CORRUPT|MISSING          version, blob_id (versión actual)
  DECOMMISSIONED             last_verified_at                   size, content_type, checksum
health HEALTHY|SUSPECT|                                         user_metadata (jsonb)
  DOWN|RECOVERING         blob_deletions (outbox)               created_at, updated_at, deleted_at
consecutive_failures      blob_id, node_id PK
last_seen_at, down_since  reason, attempts, last_error     cluster_events
used_bytes, blob_count                                     id, ts, type, node_id, object_id,
                                                           message, data
```

Detalles:

- `objects_live_name`: índice único parcial `(bucket, object_key) WHERE status <> 'DELETED'`.
  Permite lápidas (auditoría) y recrear el nombre tras borrarlo.
- `object_replicas.blob_id` identifica la versión física de cada copia; la versión lógica
  está en `objects.version`.
- `blob_deletions` es una **cola persistente** (patrón *transactional outbox*): el borrado
  lógico y el encolado del borrado físico ocurren en la misma transacción, así que un borrado
  nunca se "olvida" aunque el nodo esté caído o el gateway se reinicie.
- Estados de un objeto: `PENDING` (reservado, invisible) → `ACTIVE` (visible, cada PUT
  incrementa `version`) → `DELETED` (lápida).
- Estados de una réplica: `OK` → `CORRUPT` / `MISSING` (detectado) → `OK` (reparada) o
  fila retirada (sobrante).

---

## D. API

### Objetos (plano de aplicación)

Rutas estilo S3. `/objects/...` es un atajo del bucket `default`. Las claves pueden
contener `/` (prefijos tipo carpeta). Los buckets se crean implícitamente con el primer PUT.

| Método y ruta | Descripción | Respuestas |
|---|---|---|
| `PUT /objects/{key}` · `PUT /buckets/{b}/objects/{key}` | Crea o sobrescribe (cuerpo = bytes). Cabeceras opcionales: `Content-Type`, `X-Meta-*`, `If-Match`, `If-None-Match: *` | `201` creado · `200` nueva versión · `412` precondición · `409` borrado concurrente · `413` demasiado grande · `503` menos de W nodos |
| `GET /objects/{key}` | Descarga verificada con SHA-256. Soporta `If-None-Match` | `200` · `304` · `404` · `503` (existe pero sin réplica válida accesible) |
| `HEAD /objects/{key}` | Sólo metadatos | `200` · `404` |
| `DELETE /objects/{key}` | Borrado (idempotente, como S3) | `204` siempre (`X-Deleted: true/false`) |
| `GET /objects?prefix=&limit=&after=` · `GET /buckets/{b}/objects` | Listado paginado por clave | `200` |
| `GET /buckets` | Buckets con nº de objetos y bytes | `200` |

Cabeceras de respuesta de GET/HEAD: `ETag` (= SHA-256), `Last-Modified`, `Content-Length`,
`Content-Type`, `X-Object-Id`, `X-Object-Version`, `X-Checksum-SHA256`, `X-Meta-*`.
Sólo GET: `X-Served-By` (nodo que sirvió) y `X-Read-Attempts` (p. ej.
`storage2=error,storage3=ok` o `storage1=corrupt,storage2=ok`). Sólo HEAD: `X-Replicas`.

### Operación del clúster (plano de datos / control)

| Método y ruta | Descripción |
|---|---|
| `GET /cluster/nodes` | Membresía, salud, uso de cada nodo |
| `POST /cluster/nodes/join` | Alta (lo invoca el propio nodo al arrancar) |
| `POST /cluster/nodes/{id}/drain` · `/activate` | Retirar / reincorporar un nodo |
| `PATCH /cluster/nodes/{id}` `{"weight": n}` | Cambiar capacidad relativa |
| `GET /cluster/distribution` | Réplicas por nodo (real y deseado) y estado de los objetos: `fully_replicated`, `misplaced`, `degraded`, `under_replicated`, `unavailable` |
| `GET /cluster/locate?key=&bucket=` | Ranking HRW con puntuaciones vs. réplicas reales |
| `POST /cluster/reconcile` · `POST /cluster/scrub` | Ejecutar ya un ciclo / un scrub (devuelven el informe) |
| `GET /cluster/events?limit=&type=` | Registro de eventos (fallos, reparaciones, corrupción, GC…) |
| `GET /cluster/config` · `GET /` | Parámetros y **contrato declarado** |
| `GET /ui` | Interfaz visual en vivo (HTML sin dependencias externas) |
| `GET /cluster/feed?after=N` · `GET /cluster/map` | Actividad reciente (peticiones + eventos, en memoria) y foto del estado; los usa `/ui` |

### API interna de un Storage Node (no expuesta al cliente)

`GET /health` · `GET /blobs` (inventario) · `PUT /blobs/{id}` (exige `X-Checksum-SHA256`) ·
`GET|HEAD /blobs/{id}` · `GET /blobs/{id}/digest` · `POST /blobs/{id}/push` ·
`DELETE /blobs/{id}`.

---

## E. Flujos

### PUT

```
Cliente        Gateway                       PostgreSQL          storageA   storageB
  │ PUT key ───▶│ sha256(body)                   │                  │          │
  │             │ reservar id ─────────────────▶ │ (PENDING si nuevo)│          │
  │             │ HRW(id) → [A, B, C…]           │                  │          │
  │             │ PUT /blobs/nuevo + checksum ───┼─────────────────▶│ verifica,│
  │             │ PUT /blobs/nuevo + checksum ───┼──────────────────┼─────────▶│ fsync,
  │             │ ◀──────── 2 ACK ───────────────┼──────────────────┴──────────┘ rename
  │             │ COMMIT versión (atómico) ────▶ │ ACTIVE, v+1, réplicas A,B;
  │             │                                │ réplicas de la versión vieja → cola de borrado
  │ ◀── 201 ────│
```

| Falla… | Consecuencia | Recuperación |
|---|---|---|
| antes de escribir | nada visible | el cliente reintenta; la reserva `PENDING` caduca |
| un nodo durante la escritura | se usa el siguiente de la lista HRW | la reconciliación mueve la copia a su sitio deseado |
| no se logran W copias | `503`, no se publica nada; blobs escritos → borrado best-effort | GC de huérfanos |
| el gateway antes del commit | el cliente no recibe respuesta; blobs huérfanos | GC de huérfanos tras la gracia; el cliente reintenta (PUT es idempotente en efecto) |
| el commit | transacción: todo o nada | — |
| tras el commit, antes de responder | versión publicada; el cliente puede reintentar (crea v+1 idéntica) | — |

### GET

```
Gateway: 1 consulta (objeto + réplicas, misma instantánea) → 404 si no existe
         ordenar réplicas: HEALTHY > RECOVERING > SUSPECT, luego orden HRW; saltar DOWN
         para cada réplica:
             descargar blob ── error de red → marcar SUSPECT, siguiente
             404 en el nodo ── marcar MISSING, siguiente
             sha256 ≠ checksum ── marcar CORRUPT (evento), siguiente
             OK ── responder 200 (+ read-repair si hubo copias malas)
         ninguna válida → 503 (nunca datos obsoletos o corruptos)
```

Si un PUT concurrente sustituyó la versión mientras se leía, el blob antiguo sigue
existiendo `DELETE_GRACE_SECONDS` (5 s), por lo que la lectura en curso termina con la versión
que vio (se linealiza antes del PUT). Probado con 10 000 lecturas concurrentes a 2 000
sobrescrituras sin errores.

### HEAD

Sólo metadatos (una consulta). No contacta con los Storage Nodes: responde aunque estén todos caídos.

### DELETE

```
Transacción: bloquear objeto → réplicas → cola blob_deletions → objeto DELETED (lápida)
Respuesta: 204 (desde aquí GET/HEAD/LIST dan 404)
Asíncrono: tras DELETE_GRACE, borrar físicamente en nodos accesibles;
           nodo caído → la fila se queda en la cola y se reintenta cuando vuelva;
           además, si el nodo vuelve con blobs no referenciados, el inventario los trata como huérfanos.
```

- **Cuándo se eliminan los metadatos**: de inmediato (lógicamente, con lápida).
- **Cuándo las copias**: en cuanto su nodo esté accesible (cola persistente con reintentos).
- **Cómo se evitan huérfanos**: doble red de seguridad: cola de borrados (*outbox*) + barrido
  de inventario.

### LIST

Depende **exclusivamente de PostgreSQL**: consultar los nodos sería lento, dependería de su
disponibilidad y devolvería datos no confirmados o ya borrados. Paginación por clave
(`after`), filtro por prefijo, e indica las réplicas `OK` de cada objeto.

---

## F. Modelo de fallos

| Fallo | ¿Cómo se detecta? | ¿Qué hace el sistema? | Garantía que mantiene | Recuperación |
|---|---|---|---|---|
| **Storage Node caído** (`docker compose stop storage2`) | Error de red en una operación (→ SUSPECT) y 3 sondeos fallidos (→ DOWN, ~6 s) | Lecturas de otra réplica; escrituras en los siguientes nodos HRW | Lecturas de todo objeto con ≥1 réplica viva; escrituras si hay ≥W nodos; ningún dato confirmado perdido | Tras `REPAIR_DELAY` re-replica sus copias en otros nodos |
| **Storage Node reiniciado / vuelve** | El sondeo vuelve a responder (DOWN → RECOVERING) | Reconciliación inmediata: inventario, reposición, borrados pendientes | Sus copias válidas se reutilizan (no se vuelven a copiar) | RECOVERING → HEALTHY; sobrantes retirados |
| **Nodo colgado / lento** (`docker compose pause`) | Timeouts (datos 5 s, health 1 s) | Pasa a SUSPECT al primer timeout; se evita en lecturas y escrituras | Igual que caído | Igual que caído |
| **Archivo corrupto** | SHA-256 en GET, en copia nodo→nodo o en scrub | Copia descartada, marcada `CORRUPT`, se sirve otra | El cliente nunca recibe datos corruptos | Read-repair / scrub: se reescribe desde una copia válida |
| **Réplica faltante** (disco borrado) | Inventario (fase 1) o 404 en GET | Réplica marcada `MISSING` | Se sirve desde otra copia | Se repone en la siguiente reconciliación |
| **Error durante PUT** | Error/timeout de nodo, ACKs < W | Sustituye el nodo; si no llega a W → 503 sin publicar | Atomicidad: todo o nada | Huérfanos → GC |
| **Error durante GET** | Error/timeout/404/checksum | Siguiente réplica | Nunca datos obsoletos/corruptos; 503 si no hay ninguna válida | Read-repair |
| **Error durante DELETE** (nodo caído) | El borrado físico falla o se aplaza | 204 igualmente; blob en la cola | 404 inmediato para todos los clientes | Reintentos al volver el nodo + barrido de huérfanos |
| **Metadatos inconsistentes** | Inventario (blob sin fila / fila sin blob), reservas PENDING viejas | GC de huérfanos, `MISSING` + reposición, limpieza de reservas | Invariante metadatos ⇔ datos | Reconciliación periódica |
| **Nuevo Storage Node** | *Join* | Entra en la ubicación deseada | Ningún objeto deja de estar disponible durante el movimiento | Migración con límite por ciclo; se retira la copia vieja sólo tras confirmar la nueva |
| **Retirada de un nodo** | `drain` (admin) | Copia sus datos al resto | RF mantenido en todo momento | DECOMMISSIONED cuando queda vacío |
| **Dos nodos caídos** (RF=2) | Igual | PUT → 503; GET sirve lo que tenga copia en el nodo vivo; el resto 503 | Nada confirmado se corrompe ni se sirve mal; objetos con ambas copias en los nodos caídos: **no disponibles** hasta que vuelva uno | Al volver, reconciliación |
| **Gateway reiniciado** | — | Todo el estado está en PostgreSQL; al arrancar re-sondea nodos | Sin pérdida | Automática |
| **PostgreSQL caído** | Error de conexión | 503 en toda operación (y `/health` del gateway falla) | Consistencia sobre disponibilidad | Automática al volver (el pool reconecta) |

---

## G. Docker

- `docker compose up -d --build` levanta: `metadata-db` (PostgreSQL 17), `gateway`
  (puerto 8080), `storage1..3`. `storage4` está en el perfil `scale` y sólo arranca a petición.
- **Un volumen por nodo** (`storage1-data`, `storage2-data`, …, `metadata-data`): ningún
  nodo ve el sistema de ficheros de otro. Simula una máquina por nodo; la única forma de que un
  dato pase de un nodo a otro es la red (push nodo→nodo).
- **Redes**: `public` (cliente ↔ gateway) e `internal` (gateway ↔ nodos ↔ PostgreSQL). Los
  nodos no publican puertos: el cliente no puede saltarse el gateway.
- **Healthchecks** en todos los servicios; el gateway espera a PostgreSQL (`depends_on:
  service_healthy`); los nodos se registran solos (reintentan hasta que el gateway responde).
- Contenedores sin root, `init: true` (señales correctas), imágenes `python:3.13-slim`.
- Todos los parámetros del contrato se ajustan por variables de entorno (ver README).

## H. Demo

Guion completo en [`docs/DEMO.md`](DEMO.md); se automatiza con `./scripts/demo.sh` y se
verifica con `python3 tests/e2e_test.py` (21 comprobaciones).

La **interfaz visual** (`/ui`) muestra el sistema en vivo: el gateway registra cada petición y cada
evento en un *feed* en memoria (`/cluster/feed`) y la página anima su recorrido real sobre una
"foto" del estado (`/cluster/map`: nodos, objetos, réplicas, huecos según HRW y borrados
pendientes). Es sólo observación: no participa en el plano de datos ni altera las garantías.

---

## Limitaciones conocidas (y cómo se abordarían)

| Limitación | Motivo | Evolución |
|---|---|---|
| PostgreSQL es punto único de fallo | Simplicidad: transacciones ACID sin consenso propio | Réplica en *streaming* + failover (Patroni) o metadatos distribuidos (Raft/etcd) |
| Un solo gateway (detector de fallos único) | Evita desacuerdos entre observadores | Varios gateways sin estado detrás de un balanceador; la reconciliación ya usa *advisory lock* |
| Objeto completo en memoria del gateway | Verificar el checksum antes de enviar un solo byte | Troceado en bloques con checksum por bloque (multipart) |
| Replicación completa (overhead ×2) | Fácil de explicar y reparar | Erasure coding (Reed-Solomon), como MinIO o Ceph EC pools |
| Sin autenticación ni cifrado | Fuera del alcance | Firmas tipo S3 SigV4, TLS |
| Reconciliación recorre todos los objetos | Suficiente para miles de objetos | Particionar en grupos (como los *placement groups* de Ceph) |

## Referencias arquitectónicas (qué tomamos y qué no)

| Sistema | Tomado como referencia | Diferencia en P16 |
|---|---|---|
| **Amazon S3** | Modelo bucket/clave, ETag, `If-Match`/`If-None-Match`, metadatos `x-amz-meta-*` (aquí `X-Meta-*`), DELETE idempotente, consistencia lectura-tras-escritura fuerte | API mucho más pequeña; sin versionado expuesto |
| **Ceph (RADOS + RGW)** | Ubicación calculada (CRUSH/straw2 ≈ HRW ponderado), *down vs out*, *scrub/deep-scrub*, *backfill* con límite, gateway de objetos separado del almacenamiento | Sin monitores Paxos ni OSDs que se coordinan entre sí: un coordinador + PostgreSQL |
| **MinIO** | Clúster pequeño, interfaz estilo S3, *healing* de objetos, `bitrot` checksums | MinIO no tiene servicio de metadatos central (usa erasure coding y quórum); P16 prioriza la simplicidad |
| **GFS / HDFS** (referencia adicional) | *Master* con metadatos + servidores de datos tontos, re-replicación, GC perezoso de huérfanos, checksums | Muy similar en espíritu; P16 usa HRW en lugar de decidir cada ubicación a mano |
| **Dynamo** (referencia adicional) | Lista de preferencia, *sloppy quorum*, *read-repair*, anti-entropía | Sin versiones vectoriales: el orden lo da PostgreSQL |
