# P16 — Guion de la demostración (defensa)

Duración aproximada: 15–20 minutos. Todo se puede lanzar paso a paso con
`./scripts/demo.sh` (ENTER para avanzar) o a mano con los comandos de abajo.

**Preparación (antes de la defensa)**

```bash
docker compose down -v                 # empezar de cero
FAST=1 ./scripts/demo.sh               # o, a mano:
REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3 docker compose up -d --build --wait
```

Con `FAST=1` la re-replicación empieza a los 10 s en vez de 30 s y la reconciliación corre
cada 3 s: la demo no tiene esperas largas. Abre la **interfaz visual** y proyéctala:

```bash
python3 scripts/p16.py watch           # abre http://localhost:8080/ui
```

Todo lo que se hace en la terminal se ve animado en ella (recorrido de cada petición, copias
que viajan entre nodos, nodos caídos, corrupción…). Si hace falta explicar con calma, pon la
velocidad en **lenta** (arriba a la derecha).

---

## 1. Arranque y arquitectura (≈2 min)

```bash
docker compose ps
python3 scripts/p16.py status
curl -s localhost:8080/ | head -30          # contrato declarado
```

Qué contar: un gateway sin estado (API + coordinador), 3 nodos independientes con **su propio
volumen**, PostgreSQL como fuente de verdad de los metadatos. Los nodos se han dado de alta
solos (`node.joined` en los eventos).

## 2. Distribución y replicación (≈2 min)

```bash
curl -s -X PUT --data-binary @docker-compose.yml localhost:8080/objects/docs/compose.yml
python3 scripts/p16.py seed 12
python3 scripts/p16.py status
python3 scripts/p16.py locate docs/compose.yml
```

Qué contar:
- La respuesta del PUT dice **dónde** quedó cada copia y la garantía (`durability`).
- `locate` muestra el ranking HRW con las puntuaciones: los 2 primeros son la ubicación
  deseada; el resto, los sustitutos.
- `status`: las réplicas se reparten entre los 3 nodos (columna REPARTO ≈ DESEADO).
- Para enseñar los ficheros reales: `docker compose exec storage1 ls -l /data/blobs`.

## 3. GET / HEAD / LIST (≈1 min)

```bash
curl -si localhost:8080/objects/docs/compose.yml | head -12   # X-Served-By, X-Read-Attempts, ETag
curl -sI localhost:8080/objects/docs/compose.yml               # X-Replicas
python3 scripts/p16.py ls
```

## 4. Fallo de un nodo: disponibilidad (≈3 min)

```bash
docker compose stop storage2
python3 scripts/p16.py get demo/objeto-001.txt > /dev/null      # repetir con varios
python3 scripts/p16.py wait storage2 DOWN
python3 scripts/p16.py seed 3 durante-caida/
python3 scripts/p16.py status
```

Qué contar:
- Algunos GET muestran `intentos: storage2=error,storage1=ok`: el gateway usó la otra réplica.
- El detector pasa storage2 a `SUSPECT` (primer error) y a `DOWN` (3 sondeos, ~6 s).
- Los PUT siguen funcionando: las copias van a los siguientes nodos de la lista HRW.
- `status` muestra objetos **degradados** (1 sola copia accesible) pero ninguno inaccesible.

## 5. Reparación: re-replicación (≈2 min)

```bash
python3 scripts/p16.py wait repaired
python3 scripts/p16.py events 12
```

Qué contar: durante `REPAIR_DELAY` no se mueve nada (puede ser un reinicio; *down* vs *out*
de Ceph). Pasado ese tiempo, cada copia perdida se re-crea en otro nodo **copiando de nodo a
nodo** (`replica.re-replication storage3 -> storage1`). RF vuelve a 2 aunque storage2 siga caído.

## 6. Recuperación y reconciliación (≈2 min)

```bash
docker compose start storage2
python3 scripts/p16.py wait storage2 HEALTHY
python3 scripts/p16.py wait converged
python3 scripts/p16.py events 8
```

Qué contar: `DOWN → RECOVERING → HEALTHY`. La reconciliación compara el inventario de
storage2 con los metadatos: sus copias válidas se reutilizan, recibe lo que se escribió
mientras estaba caído (los `durante-caida/` que le tocan), se retiran las copias sobrantes y
se borran físicamente los objetos eliminados mientras no estaba.

## 7. Integridad: corrupción silenciosa (≈3 min)

```bash
python3 scripts/p16.py corrupt demo/objeto-003.txt       # sobrescribe bytes sin cambiar el tamaño
python3 scripts/p16.py get demo/objeto-003.txt           # intentos: storageX=corrupt,storageY=ok
python3 scripts/p16.py events 3                          # integrity.corrupt + replica.repair-corrupt
```

Qué contar: el fichero corrupto tiene el mismo tamaño y el disco no da error: sólo el
SHA-256 lo detecta. El cliente recibe la copia buena y la mala se repara sola (*read-repair*).

Corrupción que nadie lee:

```bash
python3 scripts/p16.py corrupt demo/objeto-004.txt
python3 scripts/p16.py scrub                             # "corrupt": [...], "repaired": 1
```

## 8. Disco perdido (≈1 min, opcional)

```bash
python3 scripts/p16.py wipe storage1        # para el nodo, borra su volumen, lo arranca vacío
python3 scripts/p16.py reconcile            # missing_detected: N, copies: repair-missing
python3 scripts/p16.py wait converged
python3 scripts/p16.py events 6             # replica.missing + replica.repair-missing
```

Con `FAST=1` el ciclo periódico (cada 3 s) puede adelantarse al `reconcile` manual: entonces
el informe sale a 0 y la reparación aparece directamente en `events`.

## 9. Cambio de membresía: añadir y retirar un nodo (≈3 min)

```bash
docker compose --profile scale up -d --wait storage4
python3 scripts/p16.py wait converged
python3 scripts/p16.py status              # storage4 ≈ 25 % de las réplicas
python3 scripts/p16.py drain storage4
python3 scripts/p16.py wait storage4 DECOMMISSIONED
docker compose --profile scale stop storage4
```

Qué contar: con HRW sólo se mueve ~1/4 de las réplicas y **todas hacia storage4**; con
`hash % N` se movería la mitad. Lo demuestra:

```bash
docker compose exec gateway python -m tools.simulate_placement
```

## 10. Cierre: garantías (≈1 min)

```bash
curl -s localhost:8080/cluster/config
python3 tests/e2e_test.py                  # 21 comprobaciones automáticas de todo lo anterior
```

---

## Preguntas probables y respuesta corta

- **¿Por qué no consenso (Raft/Paxos)?** Hay un único escritor de metadatos y una única
  fuente de verdad (PostgreSQL con transacciones). No hay decisiones que acordar entre
  pares. La exclusión mutua de la reconciliación se delega en un *advisory lock*.
- **¿Qué pasa si se cae el gateway o PostgreSQL?** Son puntos únicos de fallo asumidos.
  Los datos no se pierden (están en los nodos y en el volumen de PostgreSQL) y el servicio se
  recupera solo al volver. Evolución: réplicas de PostgreSQL y varios gateways sin estado.
- **¿Por qué no hay primario?** Los blobs son inmutables y el orden de versiones lo fija el
  commit; un primario sólo añadiría una elección de líder.
- **¿Puede un GET devolver una versión vieja?** No tras recibir el 2xx del PUT: la versión
  sale de los metadatos confirmados y cada blob se verifica con SHA-256.
- **¿Y si las dos copias están corruptas?** El GET responde 503 (nunca datos corruptos) y el
  evento `object.unavailable` lo señala. Con RF=2 ése es el límite; con RF=3 se tolera más.
- **¿Por qué esperar `REPAIR_DELAY` antes de re-replicar?** Para no mover datos por
  reinicios o cortes breves (coste de red); mientras tanto el objeto sigue legible.
