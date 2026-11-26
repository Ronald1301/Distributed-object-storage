1. Cómo llega MetadataService a ray-prototype

El PR #43 se integró en main, pero el actor no aparece en ray-prototype. ¿Puedes preparar un PR dirigido a ray-prototype con el código de MetadataService y sus dependencias? Prefiero que llevemos solo los archivos necesarios, no que mezclemos toda main en la rama Ray.

2. Ciclo de vida de MetadataService

El helper actual tiene detached=False por defecto y max_restarts=-1. ¿Qué valores quieres para R6? Propongo fijar explícitamente detached=True, un namespace estable y un límite explícito de reinicios. ¿Cómo debe conectarse el driver: con get_or_create_metadata_service(...) o con get_metadata_service(...)?

3. Interfaz de StorageNode

¿Confirmamos que el driver usará estas operaciones?
- put(blob_id, data, expected_sha256, object_id=None) → resultado con blob_id, sha256 y size.
- get(blob_id) → bytes o None; error si el checksum no coincide.
- delete(blob_id) → booleano.
- digest(blob_id) e inventory() → para verificación y estado.
¿Usamos un actor nombrado por node_id, en el namespace p16-storage-v1, y obtenemos su handle con get_named_storage_node(...)? ¿Quién mantiene el mapa node_id → actor que usará el driver?

4. Interfaz de MetadataService que necesita el driver

¿Confirmamos este flujo y estas operaciones?
- reserve(bucket, key) → reserva o devuelve el object_id.
- commit_version(...) → confirma la versión y sus réplicas.
- find_active_with_replicas(bucket, key) → metadatos más réplicas para GET.
- mark_deleted(bucket, key) → lápida lógica.
- list_objects(bucket, prefix, after, limit) → página de resultados.
commit_version devuelve actualmente una tupla (dict, bool). ¿Qué significa el booleano? ¿Qué errores o conflictos puede lanzar? ¿Qué campos de objeto y réplica devuelve el driver?

5. Comportamiento de PUT

Propongo: RF=2 y W=2; escribir en dos nodos distintos; contar como éxito solo una llamada que terminó y cuyo ray.get() no falló; y confirmar metadatos después de tener las dos réplicas. ¿Confirmas este orden?

Si un intento falla de forma ambigua, ¿reutilizamos el mismo request_id y generamos un blob_id nuevo para el intento? ¿Qué método usará MetadataService para reconocer el reintento?

6. Colocación en nodos distintos

La extensión R6 requiere que las dos réplicas queden en nodos distintos; SPREAD por sí solo no lo garantiza. ¿Fijamos cada StorageNode a un nodo Ray concreto y hacemos que el driver envíe cada réplica a un actor distinto? ¿Qué identificador del nodo espera MetadataService?

7. GET, DELETE y LIST

Para GET, ¿el driver toma las réplicas de find_active_with_replicas, prueba una por una y devuelve error solo si ninguna proporciona bytes con checksum válido?

Para DELETE, ¿R6 solo crea la lápida con mark_deleted, o también solicita el borrado físico? Para LIST, ¿usamos after como cursor exclusivo y qué respuesta debe exponer el driver?

8. Compatibilidad y pruebas

¿Podemos dejar las firmas y ejemplos de retorno escritos en docs/fase1/ o en los PRs antes de integrar? Yo pruebo el flujo de R6 y el e2e; tú validas MetadataService y su recuperación. Confírmame también si el actor ya está listo para que el driver lo importe desde ray-prototype.

En paralelo, ¿puedes revisar el PR #49 de StorageNode? Ya te tiene como revisor. Con eso puedo avanzar sobre interfaces estables mientras trabajas en la extensión #47.