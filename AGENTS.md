# AGENTS.md

Guía para agentes de código (Codex, Claude Code, etc.) que trabajen en este repositorio.

## Qué es este proyecto

Almacén de objetos distribuido (proyecto de Sistemas Distribuidos, estilo S3 simplificado):

- `gateway/`: API HTTP, coordinación, metadatos, read-repair, re-replicación y UI en `/ui`.
- `storage-node/`: nodos de almacenamiento (blobs + sidecar con checksum, módulo `blob_store`).
- `scripts/`: CLI, demo (`demo.sh`), utilidades.
- `tests/`: `e2e_test.py` (hoy) y `stress_test.py` (pendiente, tarea #7). Tests unitarios del gateway en `gateway/tests/`.
- `docs/`: `ORIENTACION.md` (enunciado oficial), `DISENO.md`, `DEMO.md`, `COMANDOS.md`.
- `escenarios/`: guiones de demo (por ejemplo `demo-completa.txt`).
- `docker-compose.yml`: levanta el sistema; el perfil `scale` añade nodos extra.

## Ramas y versiones (importante)

El repo contiene tres versiones del sistema, cada una en su rama. Comprueba en qué rama estás antes de editar:

| Rama | Qué es | Reglas |
|---|---|---|
| `main` | Rama estable. Recibe merges graduales por PR. | Sólo docs y cambios ya aceptados. |
| `native-v1` | Versión actual (gateway FastAPI + PostgreSQL + HRW). **Línea base de la Fase 2 y comparativa frente a Ray.** Tag `baseline-postgres-2026-11`. | Congelada: sólo correcciones de bugs, tests y docs. No añadas funcionalidades que acoplen más el sistema a PostgreSQL o FastAPI. |
| `ray-prototype` | Versión Ray para la entrega del 1 de diciembre (Fase 1). | Ray sólo vive aquí. Código en `ray_proto/`, documentos en `docs/fase1/`. |
| `native-v2` | Fase 2: rediseño nativo sin Ray (se crea después del 1 dic). | Sin Ray. Decisiones de diseño documentadas en `docs/fase2/`. |

Cada PR va contra la rama de la versión que modifica (no todo contra `main`). Las ramas de trabajo salen de la rama de su versión.

## Restricciones del curso (no negociables)

- **Ray sólo en `ray-prototype`.** Después de la Fase 1 Ray no puede ser dependencia de ejecución; en la memoria puede aparecer sólo como comparación.
- **No introduzcas tecnologías que automaticen o simplifiquen decisiones de diseño distribuido** (coordinación, elección de líder, consenso, descubrimiento, orden de versiones, exclusión mutua) sin que el profesor lo haya confirmado por escrito. Ante la duda: no lo añadas, descríbelo en el PR y pregunta a Ronald, que consulta al profesor. Sin respuesta, se asume que no está permitido.
- **PostgreSQL:** sólo instancia única, nunca su variante distribuida o replicada (réplicas, Patroni…). FastAPI sólo como capa HTTP. Su continuidad en la versión final **está sin decidir**: en `ray-prototype` y `native-v2` no los uses salvo indicación expresa.
- No abras issues ni PRs en `matcom/distributed-systems-2026`, ni envíes mensajes al profesor: lo hace Ronald.
- Documenta qué hace cada cosa en la app y qué hace Ray (insumo del mapa de dependencias del middleware).
- Si usas IA para generar código o documentos, mantén al día el bloque `ai_use` del repo.

Antes de cambiar nada, lee `README.md` y `docs/DISENO.md`. Si algo de esta guía no coincide con el repo, manda el repo y corrige este archivo.

## Comandos

```bash
# Tests unitarios del gateway
cd gateway && python -m unittest discover -s tests -t .

# Tests unitarios del storage node (aún no existen: tarea #5)
cd storage-node && python -m unittest discover -s tests -t .

# Lint (no hay CI todavía: tarea #3)
ruff check gateway storage-node scripts tests

# Construir y levantar
docker compose build
REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3 docker compose up -d --build --wait

# E2E (requiere el sistema levantado)
python3 tests/e2e_test.py

# Rama ray-prototype (cuando exista): ver ray_proto/README.md
#   ray start --head && python -m ray_proto.driver

# Apagar y limpiar
docker compose --profile scale down -v
```

## Reglas de trabajo

1. **Una tarea = una rama = un PR.** Ramas de trabajo: `feat/<n>-<slug>`, `fix/<n>-<slug>`, `docs/<n>-<slug>`, `test/<n>-<slug>`, `ci/<n>-<slug>` (`<n>` = número de issue).
2. **Nunca hagas push directo a `main` ni a las ramas de versión.** Todo entra por PR con 1 aprobación del compañero (hay CODEOWNERS).
3. El PR debe rellenar la plantilla: qué cambia, cómo se probó, issue relacionado (`Closes #N`).
4. Commits pequeños, en imperativo y con el número de issue: `Añade tests de blob_store (#5)`.
5. Antes de abrir el PR: lint y tests unitarios en verde. Si tocas gateway, storage node o compose, corre también el e2e.
6. No mezcles tareas: no refactorices código ajeno a la tarea. Si ves un problema fuera de alcance, descríbelo en el PR.
7. No modifiques `docs/ORIENTACION.md` (enunciado oficial).

## Convenciones de código

- Python, **sólo librería estándar en los tests** (`unittest`, sin pytest) y en `tests/stress_test.py`.
- Lint con `ruff`; no desactives reglas globalmente, usa `# noqa` puntual y justificado.
- Tests unitarios con directorios temporales (`tempfile`); nada de escribir fuera de ellos.
- Documentación y mensajes de commit en español.

## Invariantes del sistema (no los rompas)

- **Integridad:** toda escritura verifica checksum; si no coincide, se lanza `ChecksumMismatch` y **no queda ningún fichero** (ni parcial ni sidecar).
- **Escritura atómica:** se escribe en `tmp/` y se mueve; `init()` limpia restos de `tmp/`.
- **blob_id seguro:** se rechaza cualquier id con rutas (`../etc/passwd`) lanzando `InvalidBlobId`.
- **Lecturas:** un GET debe devolver siempre una versión confirmada y nunca 5xx por una réplica corrupta (read-repair).
- **Reparación:** tras la caída de un nodo, el sistema re-replica según `REPAIR_DELAY_SECONDS` y `RECONCILE_INTERVAL_SECONDS`.
- Los endpoints de caos (`/chaos`) son sólo para pruebas; por defecto el modo es `none`.

## Notas para `ray-prototype`

- Estructura propuesta: `ray_proto/storage_actor.py`, `ray_proto/metadata_actor.py`, `ray_proto/driver.py`, `ray_proto/README.md`; tests en `ray_proto/tests/`.
- Reutiliza de la versión nativa: ids ULID, checksum SHA-256, escritura atómica (temp + fsync + rename), `placement.py` (HRW) y el contrato HTTP.
- Un `ObjectRef` es transporte, nunca almacenamiento duradero. Los reintentos de Ray no son exactly-once: cada intento usa un `blob_id` nuevo y el commit de metadatos es atómico; los huérfanos se recolectan.
- Un actor reiniciado pierde su memoria: todo lo que deba sobrevivir se persiste en disco.
- Los experimentos (caída, nodo lento, concurrencia) se registran en `docs/fase1/experimentos.md` con hipótesis, resultado y evidencia.

## Seguridad

- Nunca subas claves reales. `API_KEYS` y `P16_API_KEY` se leen de variables de entorno; los ejemplos usan valores falsos.
- Si se activa autenticación, la lectura de estado para la UI puede ser pública; las operaciones de escritura no.

## Interfaz (`/ui`)

- Debe funcionar a 1366×768 y 1920×1080, en modo claro y oscuro.
- Cada estado se indica con **icono o texto además del color** (accesibilidad para daltonismo).
- Respeta `?big=1` (letra grande / alto contraste).

## Cosas que un agente NO puede hacer

Ajustes de GitHub (rama por defecto, protección de ramas), crear/proteger las ramas de versión y el tag de línea base, enviar mensajes al profesor y publicar la entrega en matcom/distributed-systems-2026, grabar el vídeo de la demo, instalar en el equipo de cada persona y revisar el trabajo del compañero requieren a una persona. Si la tarea lo necesita, deja los pasos escritos y dilo en el PR.
