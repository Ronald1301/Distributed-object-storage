# P16 — Todos los comandos

Chuleta de referencia: Docker Compose, la CLI `scripts/p16.py`, los guiones de acciones
(`run`), la API HTTP con `curl` y los tests.

## 0. Dónde se escriben

Todo se escribe en una **terminal** (app Terminal de macOS, terminal de VS Code o el panel
Terminal de la app de Claude), **dentro de la carpeta del proyecto**:

```bash
cd "/Users/agustincarbajal/Documents/Redes Distribuida"
```

Consejo: abre la **interfaz visual** (`python3 scripts/p16.py watch`, que abre
<http://localhost:8080/ui> en el navegador), déjala a la vista y lanza el resto de comandos en la
terminal: cada petición y cada fallo se ve animado. Docker Desktop tiene que estar abierto.

---

## 1. Docker Compose: levantar, observar y parar

| Comando | Qué hace |
|---|---|
| `docker compose up -d --build --wait` | Levanta gateway + storage1..3 + PostgreSQL y espera a que estén sanos |
| `REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3 docker compose up -d --build --wait` | Igual, con tiempos cortos (recomendado para pruebas y demo) |
| `docker compose ps` | Estado de los contenedores (todos deben estar `healthy`) |
| `docker compose logs -f gateway` | Log del gateway en vivo (Ctrl+C para salir) |
| `docker compose logs storage2` | Log de un nodo |
| `docker compose stop storage2` | **Tumbar** un nodo (simula máquina caída) |
| `docker compose start storage2` | Volver a arrancarlo |
| `docker compose pause storage3` | **Congelar** un nodo (sigue "vivo" pero no responde: timeouts) |
| `docker compose unpause storage3` | Descongelarlo |
| `docker compose stop storage1 storage2` | Tumbar dos nodos a la vez |
| `docker compose restart gateway` | Reiniciar el gateway (el estado está en PostgreSQL: no se pierde nada) |
| `docker compose stop metadata-db` / `start metadata-db` | Tumbar / recuperar la base de metadatos |
| `docker compose --profile scale up -d --wait storage4` | **Añadir** el 4º nodo |
| `docker compose --profile scale stop storage4` | Parar el 4º nodo (primero haz `drain`) |
| `docker compose exec storage1 ls -l /data/blobs` | Ver los ficheros reales guardados en un nodo |
| `docker compose exec metadata-db psql -U p16 -c "SELECT bucket, object_key, version, status FROM objects"` | Consultar los metadatos directamente en PostgreSQL |
| `docker compose exec metadata-db psql -U p16 -c "SELECT * FROM object_replicas"` | Ver dónde está cada réplica según los metadatos |
| `docker compose --profile scale down` | **Parar todo** (los datos se conservan) |
| `docker compose --profile scale down -v` | Parar todo y **borrar todos los datos** |

---

## 2. CLI `scripts/p16.py`

Forma general: `python3 scripts/p16.py [--bucket NOMBRE] COMANDO [argumentos]`

- `--bucket NOMBRE`: bucket sobre el que trabajar (por defecto `default`).
- Variable `P16_URL`: dirección del gateway (por defecto `http://localhost:8080`).
- Ayuda: `python3 scripts/p16.py -h` y `python3 scripts/p16.py COMANDO -h`.

### 2.1 Ver el estado

| Comando | Qué muestra |
|---|---|
| `status` | Tabla de nodos (membresía, salud, peso), reparto de réplicas real vs. deseado y estado de los objetos (completos, mal ubicados, degradados, infra-replicados, inaccesibles) |
| `watch` | Abre la **interfaz visual en vivo** en el navegador (<http://localhost:8080/ui>) |
| `watch --terminal` | Lo mismo que `status` + últimos eventos en la terminal, refrescado cada 2 s (`--interval 1`) |
| `events [N]` | Últimos N eventos (por defecto 25): caídas, reparaciones, corrupción, GC… |
| `locate CLAVE` | Ranking HRW del objeto (dónde **debería** estar) y sus réplicas reales con su estado |

### 2.2 Objetos

| Comando | Qué hace | Ejemplo |
|---|---|---|
| `put FICHERO [CLAVE]` | Sube un fichero (si no das clave usa su nombre) | `put ~/Desktop/foto.jpg fotos/foto.jpg` |
| `get CLAVE [SALIDA]` | Descarga; indica qué nodo lo sirvió y los intentos. Sin SALIDA imprime el texto | `get fotos/foto.jpg copia.jpg` |
| `verify FICHERO [CLAVE]` | Descarga y comprueba que es **idéntico** al fichero local (SHA-256). Falla si no lo es | `verify ~/Desktop/foto.jpg fotos/foto.jpg` |
| `head CLAVE` | Metadatos (tamaño, ETag, versión, réplicas) | `head fotos/foto.jpg` |
| `ls [PREFIJO]` | Lista objetos (con versión y nodos) | `ls fotos/` |
| `rm CLAVE` | Borra | `rm fotos/foto.jpg` |
| `seed N [PREFIJO]` | Sube N objetos de texto de prueba (por defecto en `demo/`) | `seed 20` |

### 2.3 Provocar fallos (para demostrar)

| Comando | Qué simula | Qué debería pasar |
|---|---|---|
| `corrupt CLAVE [NODO]` | Corrupción silenciosa: sobrescribe bytes del fichero en disco sin cambiar su tamaño | El siguiente `get`/`verify` muestra `nodo=corrupt` y sirve la copia buena; se repara sola |
| `orphan NODO` | Un blob sin metadatos (como un PUT que murió antes de confirmar) | Se borra tras `ORPHAN_GRACE_SECONDS` (60 s): evento `gc.orphan` |
| `wipe NODO` | Disco sustituido: para el nodo, borra su volumen y lo arranca vacío | La reconciliación detecta las réplicas `MISSING` y las repone |

Para tumbar, congelar o añadir nodos se usa Docker Compose (sección 1).

### 2.4 Operación del clúster

| Comando | Qué hace |
|---|---|
| `reconcile` | Ejecuta ya un ciclo de reconciliación y muestra el informe (copias, borrados, huérfanos…) |
| `scrub` | Verifica ya el SHA-256 de todas las réplicas en todos los nodos y repara lo corrupto |
| `wait converged` | Espera a que todo esté en su sitio (0 mal ubicados, 0 degradados…) |
| `wait repaired` | Espera a que ningún objeto tenga menos de 2 copias accesibles |
| `wait NODO ESTADO` | Espera a que un nodo llegue a un estado: `HEALTHY`, `SUSPECT`, `DOWN`, `RECOVERING`, `ACTIVE`, `DRAINING`, `DECOMMISSIONED`. Ej.: `wait storage2 DOWN` |
| `drain NODO` | Retira un nodo de forma ordenada: sus copias pasan a otros y queda `DECOMMISSIONED` |
| `activate NODO` | Lo vuelve a incluir en la ubicación |
| `weight NODO PESO` | Cambia su capacidad relativa (p. ej. `weight storage3 2` = doble de datos) |

`wait` admite `--timeout SEGUNDOS` (por defecto 180).

### 2.5 Guiones de acciones

| Comando | Qué hace |
|---|---|
| `run GUION [ARGS…]` | Ejecuta en orden todas las acciones escritas en un fichero de guion (ver sección 3) |
| `run GUION --yes` | Igual, sin detenerse en las líneas `pause` |
| `run GUION --keep-going` | Sigue aunque falle una acción |

---

## 3. Guiones: ejecutar varias acciones desde un fichero

Un guion es un fichero de texto con **una acción por línea**. Se ejecuta con:

```bash
python3 scripts/p16.py run escenarios/archivo.txt ~/Desktop/foto.jpg
```

### Guiones incluidos (`escenarios/`)

| Guion | Argumentos | Qué demuestra |
|---|---|---|
| `escenarios/archivo.txt` | un fichero tuyo | Ciclo completo de ese fichero: PUT → HEAD → ubicación → GET verificado → corrupción de una copia y lectura correcta → LIST → DELETE |
| `escenarios/caida-nodo.txt` | un fichero tuyo | Tu fichero sigue accesible con storage2 caído; se re-replica; storage2 vuelve y se reconcilia |
| `escenarios/membresia.txt` | — | Añadir storage4 (reequilibrado) y retirarlo (drain) |
| `escenarios/demo-completa.txt` | — | Toda la demo de la defensa, con pausas (ENTER) |

Ejemplos:

```bash
python3 scripts/p16.py run escenarios/archivo.txt ~/Desktop/foto.jpg
python3 scripts/p16.py run escenarios/caida-nodo.txt ~/Documents/informe.pdf
python3 scripts/p16.py run escenarios/membresia.txt
python3 scripts/p16.py run escenarios/demo-completa.txt
```

Al final se muestra un resumen (`■ … 16/16 acciones correctas`). Si una acción falla, el
guion se detiene y dice en qué línea.

> **El fichero que pasas tiene que existir.** La ruta es relativa a la carpeta donde estás (o
> absoluta, como `~/Desktop/foto.jpg`). Si no existe, el guion avisa y **no ejecuta nada**. Para
> crear uno de prueba en la carpeta del proyecto: `echo "hola P16" > hola.txt`.

### Formato de un guion

| Línea | Significado |
|---|---|
| `# texto` | Comentario (se ignora). Las líneas vacías también |
| `echo TEXTO` | Muestra un título |
| `put $1 pruebas/$NAME` | Cualquier comando de la CLI, **sin** `python3 scripts/p16.py` delante |
| `!docker compose stop storage2` | Comando de terminal (empieza por `!`); se ejecuta en la carpeta del proyecto |
| `sleep 5` | Espera 5 segundos |
| `pause` | Espera a que pulses ENTER (se salta con `--yes`) |
| `-get algo/que/no/existe` | Un `-` delante: si falla, el guion **continúa** |

Variables que se sustituyen:

| Variable | Valor |
|---|---|
| `$1`, `$2`, … `$9` | Argumentos que pones después del guion (`run guion.txt ARG1 ARG2`) |
| `$NAME` | Nombre (sin carpeta) del fichero `$1` |
| `$BUCKET` | Bucket en uso (`--bucket`, por defecto `default`) |

Las rutas con espacios funcionan (`"mi foto.jpg"`), tanto en los argumentos como en las acciones.

### Escribir tu propio guion

Crea por ejemplo `escenarios/mio.txt`:

```text
# Subo dos ficheros, tumbo un nodo y compruebo que ambos siguen intactos
echo Subida
put $1 mio/$NAME
put $2 mio/segundo
locate mio/$NAME

echo Se cae storage3
!docker compose stop storage3
verify $1 mio/$NAME
verify $2 mio/segundo
-put $1 mio/otra-copia

echo Vuelve storage3
!docker compose start storage3
wait storage3 HEALTHY
wait converged
status
```

y ejecútalo:

```bash
python3 scripts/p16.py run escenarios/mio.txt ~/Desktop/foto.jpg ~/Desktop/notas.txt
```

---

## 4. API HTTP con curl

### Objetos

| Operación | Comando |
|---|---|
| Subir | `curl -X PUT --data-binary @foto.jpg http://localhost:8080/objects/fotos/foto.jpg` |
| Subir con tipo y metadatos | `curl -X PUT -H "Content-Type: image/jpeg" -H "X-Meta-Autor: ana" --data-binary @foto.jpg http://localhost:8080/objects/fotos/foto.jpg` |
| Subir sólo si no existe | `curl -X PUT -H "If-None-Match: *" --data-binary @foto.jpg http://localhost:8080/objects/fotos/foto.jpg` |
| Subir a otro bucket | `curl -X PUT --data-binary @notas.txt http://localhost:8080/buckets/trabajo/objects/notas.txt` |
| Descargar | `curl -o copia.jpg http://localhost:8080/objects/fotos/foto.jpg` |
| Ver cabeceras de la descarga | `curl -s -D - -o /dev/null http://localhost:8080/objects/fotos/foto.jpg` |
| Metadatos (HEAD) | `curl -I http://localhost:8080/objects/fotos/foto.jpg` |
| Listar | `curl http://localhost:8080/objects` |
| Listar por prefijo, paginado | `curl "http://localhost:8080/objects?prefix=fotos/&limit=50&after=fotos/a.jpg"` |
| Listar otro bucket | `curl http://localhost:8080/buckets/trabajo/objects` |
| Listar buckets | `curl http://localhost:8080/buckets` |
| Borrar | `curl -X DELETE http://localhost:8080/objects/fotos/foto.jpg` |

### Clúster

| Operación | Comando |
|---|---|
| Contrato y API | `curl http://localhost:8080/` |
| Salud del gateway | `curl http://localhost:8080/health` |
| Configuración y garantías | `curl http://localhost:8080/cluster/config` |
| Nodos | `curl http://localhost:8080/cluster/nodes` |
| Reparto y estado de los objetos | `curl http://localhost:8080/cluster/distribution` |
| Dónde está un objeto | `curl "http://localhost:8080/cluster/locate?key=fotos/foto.jpg"` |
| Eventos | `curl "http://localhost:8080/cluster/events?limit=20"` (filtro: `&type=integrity`) |
| Reconciliar ya | `curl -X POST http://localhost:8080/cluster/reconcile` |
| Scrub ya | `curl -X POST http://localhost:8080/cluster/scrub` |
| Drenar un nodo | `curl -X POST http://localhost:8080/cluster/nodes/storage4/drain` |
| Reactivar un nodo | `curl -X POST http://localhost:8080/cluster/nodes/storage4/activate` |
| Cambiar peso | `curl -X PATCH -H "Content-Type: application/json" -d '{"weight": 2}' http://localhost:8080/cluster/nodes/storage3` |
| Interfaz visual (en el navegador) | <http://localhost:8080/ui> |
| Actividad en vivo que usa la interfaz | `curl "http://localhost:8080/cluster/feed?after=0"` |
| Foto completa del estado (nodos, objetos, réplicas) | `curl http://localhost:8080/cluster/map` |

Documentación interactiva (Swagger): <http://localhost:8080/docs>

---

## 5. Tests y demo

| Comando | Qué hace |
|---|---|
| `python3 tests/e2e_test.py` | 21 comprobaciones automáticas contra el clúster real (mejor con tiempos cortos) |
| `docker compose exec gateway python -m unittest discover -s tests -t .` | Tests unitarios: ubicación, identificadores y convergencia del reconciliador (dobles en memoria). En local: `cd gateway && python3 -m unittest discover -s tests -t .` |
| `docker compose exec gateway python -m tools.simulate_placement` | Compara `hash % N`, anillo consistente y HRW al pasar de 3 a 4 nodos |
| `FAST=1 ./scripts/demo.sh` | Demo guiada completa (arranca el sistema; ENTER para avanzar) |
| `python3 scripts/p16.py run escenarios/demo-completa.txt` | La misma demo como guion |

---

## 6. Secuencia típica de prueba

```bash
REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3 docker compose up -d --build --wait
python3 scripts/p16.py status
python3 scripts/p16.py run escenarios/archivo.txt ~/Desktop/foto.jpg
python3 scripts/p16.py run escenarios/caida-nodo.txt ~/Desktop/foto.jpg
python3 scripts/p16.py run escenarios/membresia.txt
python3 tests/e2e_test.py
docker compose --profile scale down -v
```
