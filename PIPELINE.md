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

Mecanismos de Ray a evaluar y usar donde aporten (se verifican contra la documentación de la versión instalada antes de depender de ellos):

| Necesidad del sistema | Mecanismo de Ray a explorar |
|---|---|
| Nodos de almacenamiento con estado | Actores con `max_restarts`, y estado duradero en disco |
| Descubrimiento de nodos | Actores con nombre |
| Escritura con quórum W=2 | Tareas en paralelo con `ray.wait(num_returns=W)` |
| Reintentos | `max_retries` de las tareas (no es exactly-once: ver riesgos) |
| Reparto de réplicas en nodos distintos | Estrategias de scheduling (`SPREAD`, afinidad) y recursos lógicos |
| Transferencia de datos entre nodos | Object store y `ObjectRef` (transporte, nunca almacenamiento duradero) |
| Observabilidad | Dashboard y API de estado de Ray |

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

- Los reintentos de Ray no son exactly-once: dos intentos de un mismo PUT pueden dejar blobs huérfanos. Mitigación: `blob_id` nuevo por intento, commit atómico de metadatos y recolección de huérfanos.
- Un actor reiniciado pierde su memoria: todo lo que deba sobrevivir se escribe en disco (temporal + fsync + renombrado).
- Un `ObjectRef` es una referencia, no almacenamiento duradero.
- El scheduling por recursos lógicos no aísla fallos.

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
