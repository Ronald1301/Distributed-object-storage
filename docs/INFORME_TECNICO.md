# Informe tecnico: almacenamiento distribuido de objetos P16

**Repositorio:** Distributed-object-storage  
**Estado descrito:** rama principal `main`  
**Stack:** Python 3.13 en contenedores, FastAPI, PostgreSQL 17 y Docker Compose v2.

## 1. Resumen ejecutivo

El proyecto implementa un servicio de almacenamiento de objetos con una API HTTP sencilla. El cliente sube y descarga objetos por clave; el sistema decide en que nodos guardar sus bytes, mantiene metadatos, verifica la integridad, detecta fallos y repara replicas.

El sistema se divide en tres partes:

1. **Gateway:** recibe las peticiones de clientes, aplica la semantica de la API y coordina los nodos.
2. **Storage Nodes:** guardan los bytes como blobs en discos/volumenes independientes.
3. **PostgreSQL:** conserva el catalogo de objetos, versiones, replicas, nodos y eventos.

Docker Compose levanta el gateway, tres nodos de almacenamiento y PostgreSQL. Un cuarto nodo opcional se puede agregar para demostrar cambios de membresia.

## 2. Arquitectura y limites de confianza

El cliente se comunica por HTTP con el gateway en el puerto 8080. El gateway se comunica por una red interna con PostgreSQL y con los Storage Nodes. Los nodos no se exponen directamente al cliente.

```text
Cliente (curl / CLI / navegador)
                |
            HTTP :8080
                |
             Gateway
             /      \
      PostgreSQL   Storage Nodes
      metadatos    blobs/replicas
```

La separacion es importante: PostgreSQL sabe que objetos existen y donde deberian estar; los Storage Nodes conservan los bytes. El contenido no se guarda dentro de PostgreSQL.

## 3. Stack y dependencias

- **Python y FastAPI:** implementan el gateway y la API de cada nodo.
- **asyncpg:** conecta el gateway con PostgreSQL de forma asincrona.
- **httpx:** permite que gateway y nodos se comuniquen por HTTP.
- **PostgreSQL 17:** fuente persistente de verdad para los metadatos.
- **Docker Compose v2:** construye y conecta todos los servicios.
- **CLI `scripts/p16.py`:** utilidad local basada principalmente en la biblioteca estandar de Python.

Las dependencias Python de cada servicio estan fijadas en `gateway/requirements.txt` y `storage-node/requirements.txt`. No hace falta instalar PostgreSQL en el host para el modo recomendado: Compose inicia el contenedor de base de datos.

## 4. Contrato de almacenamiento

| Propiedad | Decisión del proyecto |
|---|---|
| Identidad | Cada objeto logico tiene un ULID estable; cada version de bytes tiene un blob_id |
| Ubicacion | Rendezvous hashing ponderado (HRW) calcula los nodos preferidos |
| Redundancia | RF=2: dos replicas completas en nodos distintos por defecto |
| Confirmacion de escritura | W=2 por defecto: el PUT espera dos escrituras verificadas y el commit de metadatos |
| Integridad | SHA-256 al escribir, leer, copiar y durante el scrub |
| Consistencia | PostgreSQL confirma la version actual; PUT concurrentes siguen la politica de ultima escritura, con precondiciones HTTP opcionales |
| Reparacion | Reconciliacion periodica, read-repair al detectar corrupcion y re-replicacion tras una caida |

RF y W se configuran por variables de entorno. Para que el contrato por defecto de dos copias se cumpla, deben existir al menos dos nodos utilizables al escribir.

### Flujo de PUT

1. El gateway recibe el cuerpo y los metadatos HTTP.
2. Calcula SHA-256 y determina los nodos preferidos usando HRW.
3. Escribe el blob en los nodos seleccionados. El nodo usa escritura atomica, verifica el checksum y sincroniza los datos a disco.
4. Cuando se alcanzan los acknowledgements requeridos, PostgreSQL confirma el objeto y su ubicacion.
5. El gateway devuelve la respuesta con id, version, checksum y replicas.

Si no se pueden alcanzar los acknowledgements configurados, el PUT no promete durabilidad y devuelve un error.

### Flujo de GET

1. PostgreSQL resuelve bucket, clave, version y replicas registradas.
2. El gateway intenta leer una replica accesible.
3. Verifica SHA-256 antes de enviar los bytes al cliente.
4. Si una replica falla o esta corrupta, intenta otra replica valida y puede iniciar read-repair.

La API no debe servir bytes cuyo checksum no coincide.

### DELETE

El borrado logico se refleja primero en metadatos. El borrado fisico puede quedar en una cola para completarse de forma segura, evitando que un fallo entre el cambio de metadatos y la eliminacion deje estado inconsistente.

## 5. Ubicacion y replicacion

HRW calcula una puntuacion por nodo para cada objeto y ordena los candidatos. Los pesos permiten que nodos con mayor capacidad reciban proporcionalmente mas replicas. A diferencia de guardar una ubicacion fija en el cliente, el gateway puede recalcular la ubicacion deseada cuando cambia la membresia.

El sistema compara en PostgreSQL:

- **Ubicacion deseada:** nodos que HRW selecciona actualmente.
- **Ubicacion real:** replicas registradas y su estado (`OK`, `CORRUPT`, `MISSING`).

La reconciliacion intenta que la ubicacion real converja a la deseada. Limita el numero de copias por ciclo para controlar el ritmo del movimiento.

## 6. PostgreSQL y modelo de metadatos

El esquema principal vive en `gateway/app/db.py` y se inicializa al arrancar el gateway. Entre las tablas principales estan:

- `nodes`: direccion, peso, membresia, salud y datos de uso.
- `objects`: bucket, clave, version actual, blob_id, tamaño, tipo, checksum y estado.
- `object_replicas`: nodo y estado de cada copia.
- `blob_deletions`: borrados fisicos pendientes y reintentos.
- `cluster_events`: fallos, reparaciones, corrupciones y otros eventos.

PostgreSQL actua como fuente de verdad para el catalogo. Los blobs permanecen en los volumenes de los nodos.

## 7. Deteccion, recuperacion y reconciliacion

Los nodos exponen `/health`. El monitor realiza sondeos y cuenta fallos consecutivos. La maquina de estados incluye:

- `HEALTHY`: el nodo responde correctamente.
- `SUSPECT`: hay fallos recientes y se observa antes de declararlo caido.
- `DOWN`: el nodo se considera inaccesible.
- `RECOVERING`: el nodo volvio y se esta poniendo al dia.

La membresia tiene estados `ACTIVE`, `DRAINING` y `DECOMMISSIONED`. Drenar un nodo evita nuevas asignaciones y permite mover sus replicas antes de retirarlo.

Valores por defecto documentados: reconciliacion cada 10 s, re-replicacion tras 30 s en DOWN y scrub cada 300 s. Se pueden ajustar por entorno.

## 8. Middleware HTTP: que escribimos y que aporta FastAPI

El middleware de acceso esta en `gateway/app/main.py`, con la declaracion `@app.middleware("http")` y la funcion `access_log(request, call_next)`.

**La logica del middleware la escribimos nosotros:** inicia un cronometro con `time.perf_counter()`, llama a `await call_next(request)`, y luego registra metodo, ruta, status HTTP y duracion. Algunas rutas configuradas en `QUIET_PATHS` se excluyen del log.

**FastAPI/Starlette aporta la integracion:** el decorador registra la funcion como middleware HTTP; el framework proporciona el objeto `Request` y `call_next`, que delega la peticion al siguiente middleware o al endpoint y devuelve su respuesta. El parametro podria llamarse `req`; aqui se llama `request`.

Este middleware no autentica, no modifica el cuerpo y no decide si una peticion esta autorizada. Es observabilidad de acceso, no control de acceso. Los errores de API se procesan aparte mediante handlers registrados en FastAPI.

## 9. API del gateway

### Objetos

- `PUT /objects/{key}` y `PUT /buckets/{bucket}/objects/{key}`: crear o reemplazar.
- `GET /objects/{key}`: descargar con checksum verificado.
- `HEAD /objects/{key}`: consultar metadatos sin descargar el cuerpo.
- `DELETE /objects/{key}`: borrar el objeto de forma idempotente.
- `GET /objects?prefix=&limit=&after=`: listar objetos con paginacion.
- `GET /buckets`: listar buckets.

`/objects/...` usa el bucket `default`. Los buckets adicionales aparecen con la primera escritura. Las claves pueden incluir barras.

### Operacion del cluster

- `GET /cluster/nodes`, `/cluster/distribution`, `/cluster/map`: consultar nodos y distribucion.
- `GET /cluster/locate?key=...`: ver ranking HRW y replicas conocidas.
- `GET /cluster/events` y `/cluster/feed`: consultar eventos y actividad reciente.
- `POST /cluster/reconcile` y `/cluster/scrub`: iniciar esos procesos manualmente.
- `POST /cluster/nodes/{id}/drain` o `/activate`: retirar o reactivar un nodo.
- `PATCH /cluster/nodes/{id}`: cambiar el peso.

Swagger esta en `http://localhost:8080/docs` cuando el sistema esta levantado.

## 10. Componentes del codigo

| Ruta | Responsabilidad |
|---|---|
| `gateway/app/main.py` | Arranque, lifespan, middleware de log, handlers y health |
| `gateway/app/api_objects.py` | API de objetos y traduccion HTTP |
| `gateway/app/api_cluster.py` | Endpoints de operacion del cluster |
| `gateway/app/objects_service.py` | Semantica de PUT, GET, HEAD, DELETE y LIST |
| `gateway/app/placement.py` | HRW ponderado |
| `gateway/app/membership.py` | Miembros, estado de salud y transiciones |
| `gateway/app/health_monitor.py` | Sondeos y deteccion de fallos |
| `gateway/app/reconciler.py` | Reparacion, read-repair, scrub, reequilibrado y GC |
| `gateway/app/metadata.py` y `db.py` | Consultas y esquema de PostgreSQL |
| `gateway/app/storage_client.py` | Cliente HTTP hacia Storage Nodes |
| `storage-node/app/main.py` | API interna de cada nodo |
| `storage-node/app/blob_store.py` | Escritura atomica, checksum e inventario local |
| `scripts/p16.py` | CLI de desarrollo, operacion y demo |
| `gateway/tests/` y `tests/e2e_test.py` | Pruebas unitarias y pruebas contra el cluster |

## 11. Pruebas y validacion

El repositorio incluye pruebas unitarias de ubicacion y del reconciliador, ademas de una prueba end-to-end que necesita el cluster levantado. Los dobles del reconciliador sustituyen metadata, membership y storage para probar los casos de convergencia sin iniciar servicios reales.

Comandos documentados:

```bash
docker compose exec gateway python -m unittest discover -s tests -t .
python3 tests/e2e_test.py
```

La prueba end-to-end se ejecuta contra los servicios de Docker Compose. Para mas instrucciones, consultar `docs/COMANDOS.md`.

## 12. Limitaciones y seguridad

- PostgreSQL centralizado es un punto unico de fallo asumido por el diseño actual.
- El gateway es un punto unico de entrada y el middleware de log no autentica.
- No se debe exponer el servicio a redes no confiables sin agregar autenticacion, TLS y controles de despliegue adecuados.
- La perdida de volumenes de PostgreSQL afecta el catalogo aunque los bytes sigan en nodos; la recuperacion desde sidecars es una posible mejora, no debe asumirse como garantia actual.
- Dos nodos caidos pueden impedir nuevas escrituras con W=2. Una lectura requiere al menos una replica valida y accesible.

## 13. Documentos relacionados

- `README.md`: presentacion y contrato del sistema.
- `docs/DISENO.md`: decisiones arquitectonicas y flujos detallados.
- `docs/COMANDOS.md`: referencia completa de CLI, Docker, curl y demo.
- `docs/DEMO.md`: guion para la defensa.
