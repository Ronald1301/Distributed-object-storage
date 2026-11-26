# PIPELINE: hoja de ruta del proyecto P16

Servicio distribuido de almacenamiento de objetos (Sistemas Distribuidos, Universidad de La Habana, curso 2026).
Equipo: @Ronald1301 y @agustin030902. Última actualización: 25-nov-2026.

Este documento explica **qué versiones del sistema existen, en qué rama vive cada una, qué se entrega y cuándo, y cómo trabajamos**. Si llegas al repo por primera vez, empieza aquí.

## 1. Resumen

El curso evalúa el proyecto en tres fases:

| Fase | Qué se hace | Peso |
|---|---|---|
| 1. Prototipo con Ray | Versión que **saca partido de Ray**, con arquitectura, contrato, mapa de dependencias del middleware y un experimento inicial de fallos | 15 % |
| 2. Rediseño nativo | El mismo servicio sin Ray; decisiones de diseño explícitas (descubrimiento, estado, reintentos, failover…) | 50 % |
| 3. Verificación y defensa | Campaña de fallos y carga, comparación Ray/nativa, defensa individual | 35 % |

Fechas confirmadas por el profesor: **1 de diciembre de 2026** entrega de la versión Ray; **1 de enero de 2027** entrega del diseño final.

## 2. Versiones y ramas

| Rama | Qué contiene | Estado |
|---|---|---|
| `main` | Rama estable; recibe merges graduales por PR | activa |
| `native-v1` | Versión actual: gateway FastAPI, 3 nodos de almacenamiento, PostgreSQL de una sola instancia, ubicación HRW, replicación RF=2/W=2, checksums SHA-256, reconciliación, read-repair y scrub. Tag `baseline-postgres-2026-11` | congelada; **línea base y comparativa**, no es la entrega final |
| `ray-prototype` | Versión Ray de la Fase 1 (`ray_proto/`, documentos en `docs/fase1/`) | en desarrollo hasta el 1 dic |
| `native-v2` | Rediseño nativo de la Fase 2, sin Ray y sin puntos únicos de fallo | se crea después del 1 dic |

La versión `native-v1` declara dos puntos únicos de fallo (PostgreSQL y el gateway). Por eso sirve para comparar, pero no puede ser la entrega final (ver sección 3).

## 3. Reglas del profesor que condicionan el diseño

Resumen de las aclaraciones de Roberto Martí Cedeño (23 y 25 de noviembre de 2026):

1. **Prohibido** usar tecnologías que automaticen o simplifiquen las decisiones de diseño distribuido. Ante la duda se pregunta; sin respuesta se asume que no está permitido.
2. **FastAPI** es válido, pero no cuenta como "cliente" del sistema.
3. **PostgreSQL** solo en su variante **no distribuida**.
4. **Versión Ray (1 dic):** se pueden usar PostgreSQL de una instancia y FastAPI, pero se espera un proyecto que **saque partido de Ray**, no solo uno que lo use.
5. **Entrega final:** si desde el cliente se observa que el software no funciona, o si se detecta **por diseño un único punto de fallo**, no se supera el intento.

Consecuencias que asumimos:
- La versión final **no puede depender de una sola instancia** de PostgreSQL ni de un solo gateway. Los metadatos, el orden de versiones y la exclusión mutua tendrán que resolverse con mecanismos propios replicados, y habrá más de un punto de entrada para el cliente.
- En la versión Ray, un componente único (por ejemplo, el servicio de metadatos) es aceptable si está **declarado**: su limitación alimenta el mapa de dependencias del middleware.
- Decisión por defecto de la versión Ray: metadatos en un actor propio (`MetadataService`) y no en PostgreSQL, porque así Ray hace más trabajo visible. PostgreSQL queda permitido como alternativa si hay un motivo técnico, que se anotará en `docs/fase1/`.

## 4. Fase 1: entrega del 1 de diciembre (versión Ray)

### Qué se entrega

1. Prototipo Ray funcionando (put, get, delete, list) en la rama `ray-prototype`.
2. Diagrama de arquitectura.
3. Contrato de comportamiento/API, sin detalles internos de Ray.
4. Mapa de dependencias del middleware: requisito | qué da Ray | supuesto | pregunta para la versión nativa.
5. Experimento inicial: caída de un nodo, nodo lento y concurrencia, con evidencia real.
6. Declaración de uso de IA (`ai_use`) en el repo.
7. Un issue en `matcom/distributed-systems-2026` con el tema, nombre y grupo de cada integrante y el enlace público a la solución (lo publica Ronald).

### Cómo sacarle partido a Ray

Mecanismos de Ray que usaremos donde aporten, contrastados con la documentación oficial de Ray (rama `latest`, consultada el 25-nov-2026; hay que volver a comprobar con la versión que se instale):

| Necesidad | Mecanismo | Lo que dice la documentación | Qué implica para nosotros |
|---|---|---|---|
| Nodos con estado | Actores con `max_restarts` | Por defecto vale 0 (no se reinicia); -1 reinicia sin límite. Al reiniciar se vuelve a ejecutar el constructor y el estado no se conserva; recomienda guardar checkpoints propios | Fijar `max_restarts` de forma explícita; persistir en disco y recuperar el estado en el constructor |
| Descubrimiento | Actores con nombre (`ray.get_actor`) | El nombre se acota por namespace. Un actor con nombre que no es `detached` solo se puede recuperar mientras viva el driver que lo creó. `lifetime="detached"` lo desacopla del job y obliga a matarlo con `ray.kill`. Existe `get_if_exists` | Usar `detached` para nodos y metadatos y fijar un namespace |
| Reintentos de tareas | `max_retries` | Por defecto 3 (-1 infinito, 0 desactiva). `retry_exceptions` es `False` por defecto: los errores de la aplicación no se reintentan. Reintenta si muere el worker o la máquina | La documentación consultada no habla de idempotencia; la protección (`request_id`, `blob_id` nuevo por intento) es nuestra |
| Reintentos de tareas de actor | `max_task_retries` | Por defecto 0 (a lo sumo una vez); un `RayActorError` puede lanzarse aunque la tarea sí se ejecutó. Con valor > 0 o -1 es al menos una vez | Resultado ambiguo (como el timeout ambiguo de los laboratorios): el cliente reintenta con el mismo `request_id` |
| Escritura con quórum W | `ray.wait(refs, num_returns=W, timeout=...)` | Devuelve dos listas (listas y no listas); sin `timeout` espera hasta tener `num_returns`. La página no dice que cancele las tareas no listas | Comprobar con un experimento si siguen ejecutándose. "Lista" se debe contrastar con `ray.get` para contar solo éxitos (no contrastado con la documentación) |
| Reparto de réplicas | `scheduling_strategy="SPREAD"` | "Intenta" repartir entre nodos (no garantiza). `NodeAffinitySchedulingStrategy` es de bajo nivel y tiene el flag `soft`; también existe `PlacementGroupSchedulingStrategy` | No contar con nodos distintos solo por `SPREAD`: fijar cada nodo de almacenamiento o documentar la limitación |
| Transferencia de datos | Object store y `ObjectRef` | Si muere el dueño, `OwnerDiedError` y no hay recuperación. Lo creado con `ray.put` no es recuperable; los resultados de tareas de actor solo se reconstruyen si `max_task_retries` no es 0 | Un `ObjectRef` es transporte, nunca almacenamiento duradero |
| Observabilidad | Dashboard y API de estado | No contrastado (pendiente de leer esa documentación) | Revisar antes de documentarlo en el mapa de dependencias |

Cada uso se documenta: qué hizo Ray y qué tuvo que implementar la aplicación. Esa tabla es la base del mapa de dependencias.

### Tareas y responsables

| Id | Tarea | Responsable | Puntos | Plazo |
|---|---|---|---|---|
| R1 | Ramas, tag de línea base y escenario base | @Ronald1301 | 1 | 25–26 nov |
| R2 | Actores `StorageNode` con persistencia atómica | @Ronald1301 | 3 | 26–27 nov |
| R3 | Actor `MetadataService` con snapshot/WAL | @agustin030902 | 3 | 26–27 nov |
| R4 | Experimentos de caída, nodo lento y concurrencia | @Ronald1301 | 3 | 28 nov |
| R5 | Diagrama de arquitectura | @agustin030902 | 2 | 27–28 nov |
| R6 | Operaciones put/get/delete/list con réplicas e idempotencia | @Ronald1301 | 5 | 27–28 nov |
| R7 | Contrato de comportamiento/API | @agustin030902 | 2 | 26–27 nov |
| R8 | Mapa de dependencias del middleware | @agustin030902 | 3 | 29 nov |
| R9 | `ai_use`, README y documentos de guía | @Ronald1301 | 2 | 29–30 nov |

Dependencias: R6 usa las interfaces de R2 y R3 (se acuerdan el primer día); R5 necesita el código de R2, R3 y R6; R8 necesita la evidencia de R4.

### Calendario

| Fecha | Hito |
|---|---|
| 25–26 nov | Ramas creadas; firmas de las interfaces acordadas; contrato en borrador |
| 26–27 nov | Actores de almacenamiento y `MetadataService` |
| 27–28 nov | Operaciones completas; diagrama |
| 28 nov | Experimentos ejecutados con evidencia |
| 29 nov | Mapa de dependencias; contrato final |
| 30 nov | Revisión cruzada de todo, README y `ai_use` |
| 1 dic | Publicación del issue de entrega |

### Riesgos técnicos conocidos

- Un reintento puede repetir el efecto de una operación: dos intentos de un mismo PUT pueden dejar blobs huérfanos. Mitigación: `blob_id` nuevo por intento, commit atómico de metadatos y recolección de huérfanos.
- Las tareas de actor son "a lo sumo una vez" por defecto: un error puede llegar aunque la operación se ejecutó. El cliente no puede saber si ocurrió; por eso el mismo `request_id` en cada reintento.
- Un actor reiniciado pierde su memoria; si `max_restarts` queda en 0 no se reinicia. Todo lo que deba sobrevivir se escribe en disco (temporal + fsync + renombrado) y se recupera en el constructor.
- Un actor con nombre sin `lifetime="detached"` muere con el driver que lo creó.
- Un `ObjectRef` es una referencia, no almacenamiento duradero: si muere su dueño, el objeto se pierde.
- Preguntas abiertas que resolverá el experimento R4: si `ray.wait` deja ejecutando las tareas no listas, si los reintentos conservan el mismo id de tarea (el ejemplo de la documentación sugiere que sí, sin afirmarlo), y qué ve el cliente cuando un actor cae a mitad de una operación.
- El scheduling por recursos lógicos no aísla fallos (así lo explican las clases; la página de scheduling consultada no lo afirma ni lo niega).

Fuentes de Ray consultadas: [fallos de actores](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html), [fallos de tareas](https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html), [fallos de objetos](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html), [`ray.wait`](https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html), [scheduling](https://docs.ray.io/en/latest/ray-core/scheduling/index.html), [actores con nombre](https://docs.ray.io/en/latest/ray-core/actors/named-actors.html).

## 5. Fase 2: diseño final nativo (hasta el 1 de enero)

El reparto de tareas se define después del 1 de diciembre. Los criterios ya conocidos:

- Sin Ray como dependencia de ejecución.
- **Ningún punto único de fallo por diseño**, incluido el gateway: más de un punto de entrada y metadatos replicados con mecanismos propios.
- Decisiones documentadas para: descubrimiento, estado, reintentos, failover, orden de versiones y exclusión mutua.
- Lenguaje habitual para la implementación nativa: Go (Python sigue siendo posible si se justifica).
- No usar servicios que resuelvan por nosotros el mecanismo que presentamos como aporte principal.

Alternativas de metadatos a comparar: metadatos replicados por la propia colocación HRW; líder elegido con un protocolo propio simplificado; primario con réplica (esta última tendría que demostrar que no deja un punto único de fallo).

## 6. Fase 3: verificación y defensa

- Campaña de fallos y carga con el vocabulario de escenarios F0–F21 del curso, ejecutada sobre las tres versiones (`native-v1`, `ray-prototype`, `native-v2`).
- Registro experimental con el formato del curso.
- Informe comparativo Ray/nativa.
- Defensa individual. El uso de IA se declara en `ai_use`.

## 7. Cómo trabajamos

- Una tarea = una rama = un PR, siempre contra la rama de la versión que se modifica. Nombres: `feat/<n>-<slug>`, `fix/…`, `docs/…`, `test/…`.
- Nada de push directo a `main` ni a las ramas de versión; cada PR lo aprueba la otra persona (hay CODEOWNERS).
- Cada tarea tiene su issue; el PR dice `Closes #N` y cómo se probó.
- Reglas para agentes de código en [`AGENTS.md`](AGENTS.md); prompts por tarea en [`PLAN_CODEX.md`](PLAN_CODEX.md); lista de issues en [`ISSUES_PLAN.md`](ISSUES_PLAN.md).
- Las preguntas al profesor las canaliza Ronald; las respuestas se resumen en la sección 3 con su fecha.

## 8. Criterios de "hecho" para la entrega final

- [ ] El cliente puede subir, leer, listar y borrar objetos mientras cae cualquier nodo individual.
- [ ] No hay ningún componente cuya caída detenga el servicio (se comprueba matando cada contenedor o proceso por turno).
- [ ] Las decisiones de diseño están justificadas y la campaña de fallos las respalda.
- [ ] La comparación Ray/nativa usa la misma batería de escenarios.
- [ ] Cada integrante sabe defender todo el sistema.

## 9. Documentos relacionados

| Archivo | Contenido |
|---|---|
| `README.md` | Cómo levantar y usar cada versión |
| `docs/DISENO.md` | Diseño de la versión `native-v1` |
| `docs/ORIENTACION.md` | Enunciado oficial (no se modifica) |
| `docs/fase1/` | Entregables de la Fase 1 (contrato, arquitectura, experimentos, mapa de dependencias) |
| `AGENTS.md`, `PLAN_CODEX.md`, `ISSUES_PLAN.md` | Guía de trabajo del equipo y de los agentes |
