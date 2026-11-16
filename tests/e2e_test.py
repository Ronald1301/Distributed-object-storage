#!/usr/bin/env python3
"""Prueba end-to-end de P16 contra el clúster real de Docker Compose.

Sólo usa la librería estándar. Ejecuta los escenarios de la demo y verifica
automáticamente las garantías declaradas:

    1. PUT/GET/HEAD/LIST/overwrite/precondiciones      (API + durabilidad)
    2. distribución entre los 3 nodos                  (placement)
    3. caída de un nodo: lecturas y escrituras siguen   (disponibilidad)
    4. DELETE con un nodo caído                         (borrado diferido)
    5. re-replicación tras REPAIR_DELAY                 (reparación)
    6. vuelta del nodo: reconciliación                  (anti-entropía)
    7. corrupción silenciosa: GET + scrub               (integridad)
    8. alta de storage4 y drenado                       (membresía / reequilibrado)

Uso (con el clúster levantado; tiempos cortos recomendados):
    REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3 docker compose up -d --build --wait
    python3 tests/e2e_test.py
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("P16_URL", "http://localhost:8080")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUCKET = "e2e"
N_OBJECTS = 30

passed = 0


# ------------------------------------------------------------------ utilidades
def http(method: str, path: str, body: bytes | None = None, headers: dict | None = None, timeout: float = 20):
    req = urllib.request.Request(BASE + path, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.headers, err.read()


def jget(path: str) -> dict:
    status, _, body = http("GET", path)
    assert status == 200, f"GET {path} -> {status} {body[:300]!r}"
    return json.loads(body)


def jpost(path: str) -> dict:
    status, _, body = http("POST", path, b"")
    assert status == 200, f"POST {path} -> {status} {body[:300]!r}"
    return json.loads(body)


def obj_path(key: str) -> str:
    return f"/buckets/{BUCKET}/objects/{urllib.parse.quote(key)}"


def compose(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def wait_until(description: str, predicate, timeout: float, interval: float = 1.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (AssertionError, OSError, KeyError, ValueError) as exc:
            last = exc
        time.sleep(interval)
    raise AssertionError(f"Timeout esperando: {description} (último valor: {last!r})")


def step(title: str) -> None:
    print(f"\n\033[1m▶ {title}\033[0m")


def ok(message: str) -> None:
    global passed
    passed += 1
    print(f"  \033[32m✔\033[0m {message}")


def node_health(node_id: str) -> str | None:
    for node in jget("/cluster/nodes")["nodes"]:
        if node["node_id"] == node_id:
            return node["health"]
    return None


def node_state(node_id: str) -> dict | None:
    return next((n for n in jget("/cluster/nodes")["nodes"] if n["node_id"] == node_id), None)


def distribution() -> dict:
    return jget("/cluster/distribution")


def locate(key: str) -> dict:
    return jget(f"/cluster/locate?bucket={BUCKET}&key={urllib.parse.quote(key)}")


def converged() -> bool:
    d = distribution()["objects"]
    return d["under_replicated"] == 0 and d["misplaced"] == 0 and d["unavailable"] == 0


def corrupt(node_id: str, blob_id: str) -> None:
    """Simula corrupción silenciosa: sobrescribe bytes del fichero SIN cambiar su tamaño."""
    compose(
        "exec", "-T", node_id, "sh", "-c",
        f"printf 'CORRUPTED!' | dd of=/data/blobs/{blob_id} bs=1 seek=0 conv=notrunc 2>/dev/null",
    )


# ------------------------------------------------------------------ escenarios
def main() -> int:
    cfg = jget("/cluster/config")["settings"]
    repair_delay = cfg["repair_delay_s"]
    cycle = cfg["reconcile_interval_s"]
    detect = cfg["health_interval_s"] * (cfg["down_after"] + 2)
    print(f"Gateway {BASE} · RF={cfg['replication_factor']} W={cfg['write_acks']} "
          f"repair_delay={repair_delay}s reconcile={cycle}s")

    step("0. Preparación")
    wait_until("3 nodos HEALTHY", lambda: all(
        node_health(n) == "HEALTHY" for n in ("storage1", "storage2", "storage3")), 60)
    for storage4_state in [node_state("storage4")]:
        if storage4_state and storage4_state["membership"] != "DECOMMISSIONED":
            raise SystemExit("storage4 sigue en el clúster de una ejecución anterior; ejecute: "
                             "docker compose --profile scale rm -sf storage4 y drénelo/retírelo")
    listing = jget(f"/buckets/{BUCKET}/objects?limit=1000")
    for obj in listing["objects"]:
        http("DELETE", obj_path(obj["key"]))
    ok(f"clúster listo; {listing['count']} objetos antiguos del bucket '{BUCKET}' eliminados")

    step("1. PUT / GET / HEAD / LIST")
    rnd = random.Random(16)
    contents: dict[str, bytes] = {}
    for i in range(N_OBJECTS):
        key = f"docs/obj-{i:02d}.bin"
        data = rnd.randbytes(rnd.randint(64, 20_000))
        status, headers, body = http("PUT", obj_path(key), data, {"Content-Type": "application/octet-stream",
                                                                   "X-Meta-Autor": "e2e"})
        assert status == 201, (status, body)
        info = json.loads(body)
        assert info["checksum_sha256"] == hashlib.sha256(data).hexdigest()
        assert len(set(info["replicas"])) == cfg["replication_factor"], info
        contents[key] = data
    ok(f"{N_OBJECTS} objetos creados (201), cada uno con {cfg['replication_factor']} réplicas distintas")

    for key, data in contents.items():
        status, headers, body = http("GET", obj_path(key))
        assert status == 200 and body == data, (key, status)
    ok("GET devuelve exactamente los bytes escritos")

    key0 = "docs/obj-00.bin"
    status, headers, _ = http("HEAD", obj_path(key0))
    assert status == 200 and int(headers["Content-Length"]) == len(contents[key0])
    assert headers["ETag"].strip('"') == hashlib.sha256(contents[key0]).hexdigest()
    assert headers["X-Meta-Autor"] == "e2e"
    ok(f"HEAD: tamaño, ETag (SHA-256), metadatos de usuario y réplicas ({headers['X-Replicas']})")

    listing = jget(f"/buckets/{BUCKET}/objects?prefix=docs/&limit=1000")
    assert listing["count"] == N_OBJECTS
    page = jget(f"/buckets/{BUCKET}/objects?prefix=docs/&limit=10")
    assert page["truncated"] and page["next_after"]
    ok("LIST por prefijo y paginación")

    new_data = b"segunda version " * 10
    status, _, body = http("PUT", obj_path(key0), new_data)
    assert status == 200 and json.loads(body)["version"] == 2
    assert http("GET", obj_path(key0))[2] == new_data
    contents[key0] = new_data
    assert http("PUT", obj_path(key0), b"x", {"If-None-Match": "*"})[0] == 412
    assert http("PUT", obj_path(key0), b"x", {"If-Match": '"0000"'})[0] == 412
    etag = f'"{hashlib.sha256(new_data).hexdigest()}"'
    assert http("GET", obj_path(key0), headers={"If-None-Match": etag})[0] == 304
    ok("sobrescritura -> versión 2; If-None-Match/If-Match -> 412; GET condicional -> 304")

    step("2. Distribución entre nodos")
    d = distribution()
    shares = {n["node_id"]: n["replicas"] for n in d["nodes"] if n["membership"] == "ACTIVE"}
    assert all(v > 0 for v in shares.values()), shares
    ok(f"réplicas por nodo: {shares}")

    step("3. Caída de storage2: disponibilidad")
    on_s2 = [k for k in contents if "storage2" in [r["node_id"] for r in locate(k)["replicas"]]]
    last_event = max((e["id"] for e in jget("/cluster/events?limit=1")["events"]), default=0)
    compose("stop", "storage2")
    for key, data in contents.items():
        status, headers, body = http("GET", obj_path(key))
        assert status == 200 and body == data, (key, status)
    ok(f"los {N_OBJECTS} objetos siguen legibles ({len(on_s2)} tenían réplica en storage2)")
    wait_until("storage2 DOWN", lambda: node_health("storage2") == "DOWN", detect + 10)
    ok("el detector de fallos marca storage2 como DOWN")

    extra = {}
    for i in range(5):
        key = f"docs/durante-caida-{i}.txt"
        data = f"escrito con storage2 caído #{i}".encode() * 5
        status, _, body = http("PUT", obj_path(key), data)
        info = json.loads(body)
        assert status == 201 and "storage2" not in info["replicas"] and len(info["replicas"]) == 2, info
        extra[key] = data
    contents.update(extra)
    ok("PUT sigue funcionando con 2 nodos: las réplicas van a los siguientes de la lista HRW")

    deleted_key = on_s2[0]
    deleted_blobs = {r["node_id"]: r["blob_id"] for r in locate(deleted_key)["replicas"]}
    assert http("DELETE", obj_path(deleted_key))[0] == 204
    assert http("GET", obj_path(deleted_key))[0] == 404
    contents.pop(deleted_key)
    backlog = {b["node_id"]: b["pending"] for b in distribution()["deletion_backlog"]}
    assert backlog.get("storage2", 0) >= 1, backlog
    ok(f"DELETE con storage2 caído: 404 inmediato, borrado físico pendiente en storage2 ({backlog})")

    step(f"4. Re-replicación tras REPAIR_DELAY ({repair_delay}s)")
    d = distribution()["objects"]
    assert d["degraded"] > 0, d
    ok(f"{d['degraded']} objetos degradados (1 sola copia accesible); durante la gracia no se re-replica")
    wait_until("re-replicación completa", lambda: distribution()["objects"]["degraded"] == 0,
               repair_delay + 4 * cycle + 20, 2)
    assert node_health("storage2") == "DOWN"
    rerep = [e for e in jget("/cluster/events?type=replica.re-replication&limit=500")["events"]
             if e["id"] > last_event]
    assert rerep, "no hubo eventos de re-replicación"
    ok(f"con storage2 aún caído, {len(rerep)} copias se re-crean en los nodos restantes (RF restaurado)")

    step("5. Vuelta de storage2: reconciliación")
    compose("start", "storage2")
    wait_until("storage2 HEALTHY", lambda: node_health("storage2") == "HEALTHY", 60)
    wait_until("estado convergido", converged, 4 * cycle + 30, 2)
    wait_until("cola de borrados vacía", lambda: not distribution()["deletion_backlog"], 4 * cycle + 30, 2)
    listing = compose("exec", "-T", "storage2", "ls", "/data/blobs")
    assert deleted_blobs.get("storage2") not in listing
    ok("storage2 vuelve, recibe sus réplicas deseadas y borra el objeto eliminado mientras estaba caído")
    for key, data in contents.items():
        assert http("GET", obj_path(key))[2] == data
    ok("todos los objetos íntegros tras la reconciliación")

    step("6. Corrupción silenciosa detectada en GET (read-repair)")
    key = "docs/obj-05.bin"
    served_by = http("GET", obj_path(key))[1]["X-Served-By"]
    blob_id = locate(key)["object"]["blob_id"]
    corrupt(served_by, blob_id)
    status, headers, body = http("GET", obj_path(key))
    assert status == 200 and body == contents[key]
    assert f"{served_by}=corrupt" in headers["X-Read-Attempts"], headers["X-Read-Attempts"]
    ok(f"copia de {served_by} corrupta -> detectada y descartada; servida por {headers['X-Served-By']}")
    wait_until("réplica reparada", lambda: all(r["status"] == "OK" for r in locate(key)["replicas"]), 15)
    scrub = jpost("/cluster/scrub")
    assert not scrub["corrupt"], scrub
    ok("read-repair: la copia corrupta fue reemplazada; el scrub confirma 0 corruptas")

    step("7. Corrupción detectada por el scrub (sin lecturas)")
    key = "docs/obj-07.bin"
    rep = locate(key)["replicas"][0]
    corrupt(rep["node_id"], rep["blob_id"])
    scrub = jpost("/cluster/scrub")
    assert len(scrub["corrupt"]) == 1 and scrub["repaired"] >= 1, scrub
    assert not jpost("/cluster/scrub")["corrupt"]
    assert http("GET", obj_path(key))[2] == contents[key]
    ok(f"scrub detecta la réplica corrupta en {rep['node_id']} y la repara desde otra copia")

    step("8. Membresía: alta de storage4 y reequilibrado")
    compose("--profile", "scale", "up", "-d", "--wait", "storage4")
    wait_until("storage4 HEALTHY", lambda: node_health("storage4") == "HEALTHY", 60)
    wait_until("reequilibrado completo", converged, 6 * cycle + 30, 2)
    d = distribution()
    s4 = next(n for n in d["nodes"] if n["node_id"] == "storage4")
    assert s4["replicas"] > 0
    ok(f"storage4 recibe {s4['replicas']} réplicas ({s4['share']} del total; deseado {s4['desired_share']})")

    jpost("/cluster/nodes/storage4/drain")
    wait_until("storage4 DECOMMISSIONED",
               lambda: node_state("storage4")["membership"] == "DECOMMISSIONED", 6 * cycle + 30, 2)
    assert converged()
    compose("--profile", "scale", "rm", "-sf", "storage4")
    for key, data in contents.items():
        assert http("GET", obj_path(key))[2] == data
    ok("drenado: sus datos se mueven al resto y el nodo se retira sin pérdida")

    step("Limpieza")
    for key in contents:
        http("DELETE", obj_path(key))
    ok(f"bucket '{BUCKET}' vaciado")

    print(f"\n\033[32m{passed} comprobaciones superadas\033[0m")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"\n\033[31m✘ FALLO: {exc}\033[0m")
        sys.exit(1)
