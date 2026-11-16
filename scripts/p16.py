#!/usr/bin/env python3
"""CLI de ayuda para usar y demostrar P16 (sólo librería estándar).

    python3 scripts/p16.py status              estado de nodos y distribución
    python3 scripts/p16.py watch               abre la interfaz visual en vivo (http://localhost:8080/ui)
    python3 scripts/p16.py watch --terminal    estado refrescado cada 2 s en la terminal
    python3 scripts/p16.py put FICHERO [CLAVE] subir un fichero
    python3 scripts/p16.py get CLAVE [SALIDA]  descargar (muestra qué nodo sirvió)
    python3 scripts/p16.py head CLAVE          metadatos
    python3 scripts/p16.py ls [PREFIJO]        listar
    python3 scripts/p16.py rm CLAVE            borrar
    python3 scripts/p16.py seed N [PREFIJO]    subir N objetos de prueba
    python3 scripts/p16.py locate CLAVE        ubicación deseada (HRW) vs. réplicas reales
    python3 scripts/p16.py events [N]          últimos eventos del clúster
    python3 scripts/p16.py corrupt CLAVE [NODO]  corromper una réplica en disco (demo integridad)
    python3 scripts/p16.py orphan NODO         crear un blob sin metadatos en un nodo (demo GC)
    python3 scripts/p16.py wipe NODO           "cambiar el disco" de un nodo (borra su volumen)
    python3 scripts/p16.py wait converged      esperar a que todo esté en su sitio (o: wait repaired,
                                               wait storage2 DOWN, wait storage4 DECOMMISSIONED)
    python3 scripts/p16.py reconcile | scrub   forzar reconciliación / scrub
    python3 scripts/p16.py drain NODO | activate NODO | weight NODO PESO
    python3 scripts/p16.py verify FICHERO [CLAVE]  descargar y comparar con el fichero local (SHA-256)
    python3 scripts/p16.py run GUION [ARGS...]     ejecutar en orden las acciones de un fichero de guion
                                                   (ver escenarios/ y docs/COMANDOS.md)

Opciones globales: --bucket NOMBRE (por defecto 'default'), P16_URL=http://localhost:8080
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser

BASE = os.environ.get("P16_URL", "http://localhost:8080")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GREEN, RED, YELLOW, BLUE, DIM, BOLD, RESET = (
    "\033[32m", "\033[31m", "\033[33m", "\033[36m", "\033[2m", "\033[1m", "\033[0m"
)
HEALTH_COLOR = {"HEALTHY": GREEN, "SUSPECT": YELLOW, "DOWN": RED, "RECOVERING": BLUE}


# ---------------------------------------------------------------------- HTTP
def request(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(BASE + path, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.headers, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.headers, err.read()
    except urllib.error.URLError as err:
        sys.exit(f"{RED}No se puede conectar con {BASE}: {err.reason}{RESET}\n¿Está levantado? docker compose up -d")


def jcall(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else (b"" if method != "GET" else None)
    status, _, raw = request(method, path, data, {"Content-Type": "application/json"} if body else {})
    payload = json.loads(raw) if raw else {}
    if status >= 400:
        sys.exit(f"{RED}HTTP {status}: {json.dumps(payload, ensure_ascii=False)}{RESET}")
    return payload


def obj_path(bucket: str, key: str) -> str:
    return f"/buckets/{bucket}/objects/{urllib.parse.quote(key)}"


def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True)


# ------------------------------------------------------------------- estado
def render_status() -> str:
    nodes = jcall("GET", "/cluster/nodes")["nodes"]
    dist = jcall("GET", "/cluster/distribution")
    by_id = {n["node_id"]: n for n in dist["nodes"]}
    lines = [
        f"{BOLD}P16 · RF={dist['replication_factor']} · W={dist['write_acks']} · {BASE}{RESET}",
        "",
        f"{'NODO':<10} {'MEMBRESÍA':<15} {'SALUD':<11} {'PESO':>4}  {'RÉPLICAS':>8}  "
        f"{'REPARTO':<24} {'DESEADO':>7}",
    ]
    for n in nodes:
        d = by_id.get(n["node_id"], {})
        share = float(d.get("share", "0%").rstrip("%"))
        bar = "█" * round(share / 100 * 20)
        color = HEALTH_COLOR.get(n["health"], "")
        if n["membership"] == "DECOMMISSIONED":  # retirado: ya no se sondea
            n["health"], color = "—", DIM
        member = n["membership"] if n["membership"] == "ACTIVE" else f"{YELLOW}{n['membership']}{RESET}"
        pad = 15 + (len(member) - len(n["membership"]))
        lines.append(
            f"{n['node_id']:<10} {member:<{pad}} {color}{n['health']:<11}{RESET} {n['weight']:>4g}  "
            f"{d.get('replicas', 0):>8}  {bar:<20}{d.get('share', ''):>6} {d.get('desired_share', ''):>7}"
        )
    o = dist["objects"]
    warn = lambda v: f"{RED}{v}{RESET}" if v else f"{v}"  # noqa: E731
    lines += [
        "",
        f"Objetos: {o['total']} · completos {o['fully_replicated']} · mal ubicados {warn(o['misplaced'])}"
        f" · degradados {warn(o['degraded'])} · infra-replicados {warn(o['under_replicated'])}"
        f" · inaccesibles {warn(o['unavailable'])}",
    ]
    backlog = ", ".join(f"{b['node_id']}={b['pending']}" for b in dist["deletion_backlog"]) or "ninguno"
    lines.append(f"Borrados físicos pendientes: {backlog}")
    return "\n".join(lines)


def cmd_status(_: argparse.Namespace) -> None:
    print(render_status())


def cmd_watch(args: argparse.Namespace) -> None:
    if not args.terminal:
        url = BASE + "/ui"
        jcall("GET", "/health")  # comprueba que el gateway responde
        print(f"{BOLD}Interfaz visual en vivo:{RESET} {url}")
        if webbrowser.open(url):
            print("Abierta en tu navegador. Déjala a la vista y lanza curl o p16.py en otra terminal.")
        else:
            print("Ábrela en tu navegador copiando la dirección de arriba.")
        print(f"{DIM}(vista en la terminal: python3 scripts/p16.py watch --terminal){RESET}")
        return
    try:
        while True:
            screen = render_status()
            events = jcall("GET", "/cluster/events?limit=8")["events"]
            screen += f"\n\n{BOLD}Últimos eventos{RESET}\n" + "\n".join(format_event(e) for e in reversed(events))
            print("\033[2J\033[H" + screen + f"\n\n{DIM}(Ctrl+C para salir){RESET}", flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass


def format_event(e: dict) -> str:
    t = e["type"]
    color = RED if t.startswith(("integrity", "node.down", "object.unavailable")) else (
        YELLOW if t.startswith(("node.suspect", "replica.missing", "gc")) else
        GREEN if t.startswith(("replica.repair", "replica.re-", "node.healthy", "node.joined")) else "")
    return f"{DIM}{e['ts'][11:19]}{RESET} {color}{t:<24}{RESET} {e['message']}"


def cmd_events(args: argparse.Namespace) -> None:
    for e in reversed(jcall("GET", f"/cluster/events?limit={args.n}")["events"]):
        print(format_event(e))


# ------------------------------------------------------------------ objetos
def missing_file_message(path: str) -> str:
    return (f"{RED}✘ No existe el fichero '{path}'{RESET} (buscado en {os.path.abspath(path)})\n"
            f"  Créalo primero, por ejemplo:  echo \"hola P16\" > {shlex.quote(path)}\n"
            f"  o pasa la ruta de uno que exista (p. ej. ~/Desktop/foto.jpg)")


def read_local(path: str) -> bytes:
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except FileNotFoundError:
        sys.exit(missing_file_message(path))
    except IsADirectoryError:
        sys.exit(f"{RED}✘ '{path}' es una carpeta, no un fichero{RESET}")


def cmd_put(args: argparse.Namespace) -> None:
    data = read_local(args.file)
    key = args.key or os.path.basename(args.file)
    status, _, body = request("PUT", obj_path(args.bucket, key), data, {})
    print(body.decode())
    if status >= 400:
        sys.exit(1)


def cmd_get(args: argparse.Namespace) -> None:
    status, headers, body = request("GET", obj_path(args.bucket, args.key))
    if status != 200:
        sys.exit(f"{RED}HTTP {status}: {body.decode()}{RESET}")
    print(f"{GREEN}200 OK{RESET} · {len(body)} B · servido por {BOLD}{headers['X-Served-By']}{RESET}"
          f" · intentos: {headers['X-Read-Attempts']} · versión {headers['X-Object-Version']}", file=sys.stderr)
    if args.out:
        with open(args.out, "wb") as fh:
            fh.write(body)
        print(f"guardado en {args.out}", file=sys.stderr)
    elif len(body) <= 2000 and _is_text(body):
        sys.stdout.write(body.decode())
        if not body.endswith(b"\n"):
            print()
    else:
        print(f"{DIM}(contenido binario o grande: p16.py get {args.key} FICHERO_SALIDA para guardarlo){RESET}",
              file=sys.stderr)


def _is_text(data: bytes) -> bool:
    try:
        data.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


def cmd_verify(args: argparse.Namespace) -> None:
    """Descarga el objeto y comprueba que es idéntico al fichero local."""
    local = hashlib.sha256(read_local(args.file)).hexdigest()
    key = args.key or os.path.basename(args.file)
    status, headers, body = request("GET", obj_path(args.bucket, key))
    if status != 200:
        sys.exit(f"{RED}✘ {key}: HTTP {status} {body.decode(errors='replace')}{RESET}")
    remote = hashlib.sha256(body).hexdigest()
    detail = (f"servido por {headers['X-Served-By']} · intentos: {headers['X-Read-Attempts']}"
              f" · versión {headers['X-Object-Version']}")
    if remote != local:
        sys.exit(f"{RED}✘ {key} NO coincide con {args.file}{RESET}\n  local  {local}\n  remoto {remote}\n  {detail}")
    print(f"{GREEN}✔ {key} idéntico a {os.path.basename(args.file)}{RESET} (sha256 {local[:16]}…) · {detail}")


def cmd_head(args: argparse.Namespace) -> None:
    status, headers, _ = request("HEAD", obj_path(args.bucket, args.key))
    print(f"HTTP {status}")
    for name, value in headers.items():
        print(f"  {name}: {value}")


def cmd_ls(args: argparse.Namespace) -> None:
    q = urllib.parse.urlencode({"prefix": args.prefix, "limit": 1000})
    data = jcall("GET", f"/buckets/{args.bucket}/objects?{q}")
    for o in data["objects"]:
        print(f"{o['key']:<40} {o['size']:>9} B  v{o['version']:<3} {','.join(o['replicas'])}")
    print(f"{DIM}{data['count']} objetos{RESET}")


def cmd_rm(args: argparse.Namespace) -> None:
    status, headers, _ = request("DELETE", obj_path(args.bucket, args.key))
    print(f"HTTP {status} (existía: {headers.get('X-Deleted')})")


def cmd_seed(args: argparse.Namespace) -> None:
    rnd = random.Random()
    for i in range(args.n):
        key = f"{args.prefix}objeto-{i:03d}.txt"
        data = (f"P16 objeto {i} · {time.time()} · " + "x" * rnd.randint(10, 4000)).encode()
        status, _, body = request("PUT", obj_path(args.bucket, key), data, {"Content-Type": "text/plain"})
        replicas = json.loads(body).get("replicas") if status < 300 else body.decode()
        print(f"PUT {key:<28} -> {status} {replicas}")


def cmd_locate(args: argparse.Namespace) -> None:
    q = urllib.parse.urlencode({"bucket": args.bucket, "key": args.key})
    print(json.dumps(jcall("GET", f"/cluster/locate?{q}"), indent=2, ensure_ascii=False))


# ------------------------------------------------------------ inyección de fallos
def cmd_corrupt(args: argparse.Namespace) -> None:
    q = urllib.parse.urlencode({"bucket": args.bucket, "key": args.key})
    info = jcall("GET", f"/cluster/locate?{q}")
    blob_id = info["object"]["blob_id"]
    replicas = [r["node_id"] for r in info["replicas"] if r["status"] == "OK"]
    node = args.node or next((n for n in info["placement"]["desired"] if n in replicas), replicas[0])
    if node not in replicas:
        sys.exit(f"{node} no tiene una réplica válida de {args.key} (réplicas: {replicas})")
    path = f"/data/blobs/{blob_id}"
    before = compose("exec", "-T", node, "sha256sum", path).stdout.split()[0]
    result = compose("exec", "-T", node, "sh", "-c",
                     f"printf 'CORRUPTED!' | dd of={path} bs=1 seek=0 conv=notrunc 2>/dev/null")
    if result.returncode != 0:
        sys.exit(result.stderr)
    after = compose("exec", "-T", node, "sha256sum", path).stdout.split()[0]
    print(f"{RED}Réplica de '{args.key}' corrompida en {node}{RESET} ({path}, mismo tamaño)")
    print(f"  sha256 antes : {before}\n  sha256 ahora : {after}\n  esperado     : {info['object']['checksum']}")
    print("Ahora: p16.py get CLAVE (read-repair)  o  p16.py scrub (detección sin lecturas)")


def cmd_orphan(args: argparse.Namespace) -> None:
    """Escribe un blob directamente en un nodo, sin metadatos (como un PUT que
    murió antes del commit). El GC lo eliminará pasado ORPHAN_GRACE_SECONDS."""
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    blob_id = "".join(random.choice(alphabet) for _ in range(26))
    data = b"blob huerfano: nunca se confirmo en los metadatos"
    code = (
        "import urllib.request,sys;"
        f"r=urllib.request.Request('http://127.0.0.1:9000/blobs/{blob_id}',data={data!r},method='PUT',"
        f"headers={{'X-Checksum-SHA256':'{hashlib.sha256(data).hexdigest()}'}});"
        "print(urllib.request.urlopen(r).status)"
    )
    result = compose("exec", "-T", args.node, "python", "-c", code)
    if result.returncode != 0:
        sys.exit(result.stderr)
    grace = jcall("GET", "/cluster/config")["settings"]["orphan_grace_s"]
    print(f"Blob huérfano {blob_id} creado en {args.node}. Se recolectará tras {grace:g} s "
          f"(ver: p16.py events)")


def cmd_wipe(args: argparse.Namespace) -> None:
    node = args.node
    volume = f"p16_{node}-data"
    print(f"Simulando reemplazo del disco de {node}: parar, borrar volumen {volume}, arrancar vacío…")
    profile = ["--profile", "scale"] if node == "storage4" else []
    for step in (["stop", node], ["rm", "-f", node]):
        compose(*profile, *step)
    subprocess.run(["docker", "volume", "rm", volume], capture_output=True, text=True)
    result = compose(*profile, "up", "-d", "--wait", node)
    if result.returncode != 0:
        sys.exit(result.stderr)
    print(f"{node} arrancado con el disco vacío. La reconciliación detectará las réplicas ausentes"
          " (replica.missing) y las repondrá desde otros nodos (o fuerce: p16.py reconcile).")


# ------------------------------------------------------------------- esperas
def cmd_wait(args: argparse.Namespace) -> None:
    """wait converged | wait repaired | wait NODO ESTADO (salud o membresía)."""
    def objects() -> dict:
        return jcall("GET", "/cluster/distribution")["objects"]

    def node(node_id: str) -> dict:
        return next((n for n in jcall("GET", "/cluster/nodes")["nodes"] if n["node_id"] == node_id), {})

    if args.what == "converged":
        desc = "todas las réplicas en su ubicación deseada"
        check = lambda: (lambda o: not (o["misplaced"] or o["under_replicated"] or o["degraded"]  # noqa: E731
                                        or o["unavailable"]))(objects())
    elif args.what == "repaired":
        desc = "ningún objeto con menos de RF copias accesibles"
        check = lambda: objects()["degraded"] == 0  # noqa: E731
    elif args.state:
        desc = f"{args.what} en estado {args.state}"
        check = lambda: args.state.upper() in (node(args.what).get("health"), node(args.what).get("membership"))  # noqa: E731
    else:
        sys.exit("uso: wait converged | wait repaired | wait NODO ESTADO")
    started = time.time()
    while time.time() - started < args.timeout:
        if check():
            print(f"\r{GREEN}✔{RESET} {desc} ({time.time() - started:.0f} s){' ' * 30}")
            return
        o = objects()
        print(f"\r… esperando: {desc} ({time.time() - started:.0f} s) · degradados {o['degraded']}"
              f" · mal ubicados {o['misplaced']}   ", end="", flush=True)
        time.sleep(1)
    sys.exit(f"\n{RED}Timeout esperando: {desc}{RESET}")


# ------------------------------------------------------------------- clúster
def cmd_simple_post(path: str):
    def run(_: argparse.Namespace) -> None:
        print(json.dumps(jcall("POST", path), indent=2, ensure_ascii=False))
    return run


# --------------------------------------------------------------- guiones
VAR_RE = re.compile(r"\$(NAME|BUCKET|\d)")


def cmd_run(args: argparse.Namespace) -> None:
    """Ejecuta en orden las acciones de un fichero de guion.

    Formato (una acción por línea):
        # comentario             línea ignorada
        echo TEXTO               muestra un título
        put $1 pruebas/$NAME     cualquier comando de esta CLI (sin "python3 scripts/p16.py")
        !docker compose stop storage2   comando de terminal (se ejecuta en la carpeta del proyecto)
        sleep 5                  esperar 5 segundos
        pause                    esperar ENTER (se omite con --yes)
        -COMANDO                 un "-" delante: si falla, el guion sigue
    Variables: $1..$9 = argumentos tras el guion, $NAME = nombre del fichero $1, $BUCKET.
    """
    with open(args.script, encoding="utf-8") as fh:
        steps = [(n, line.strip()) for n, line in enumerate(fh, 1)
                 if line.strip() and not line.strip().startswith("#")]
    # rutas absolutas para ejecutar (los comandos "!" corren en la carpeta del proyecto);
    # en pantalla se muestran tal como se escribieron
    real_params = [os.path.abspath(p) if os.path.exists(p) else p for p in args.params]

    def expand(text: str, lineno: int, quote: bool = False, display: bool = False) -> str:
        params = args.params if display else real_params

        def repl(m: re.Match) -> str:
            name = m.group(1)
            if name == "BUCKET":
                value = args.bucket
            elif name == "NAME":
                value = os.path.basename(params[0]) if params else ""
            elif name == "0":
                value = args.script
            elif int(name) <= len(params):
                value = params[int(name) - 1]
            else:
                sys.exit(f"{RED}{args.script}:{lineno}: el guion usa ${name} pero no se pasó ese argumento\n"
                         f"uso: p16.py run {args.script} ARG1 ARG2 …{RESET}")
            return shlex.quote(value) if quote else value
        return VAR_RE.sub(repl, text)

    # Antes de ejecutar nada (p. ej. tumbar un nodo), comprobar que existen los ficheros
    # locales que el guion va a subir o verificar.
    for lineno, raw in steps:
        tokens = shlex.split(raw.lstrip("-").strip())
        if len(tokens) >= 2 and tokens[0] in ("put", "verify"):
            path = expand(tokens[1], lineno, display=True)
            if not os.path.isfile(path):
                sys.exit(f"{missing_file_message(path)}\n  ({args.script}:{lineno}: {raw}) · no se ha ejecutado nada")

    parser = build_parser()
    total = sum(1 for _, raw in steps if not raw.startswith("echo"))
    idx, failures, started = 0, 0, time.time()
    print(f"{BOLD}▶ {args.script}{RESET} ({total} acciones)", flush=True)
    for lineno, raw in steps:
        optional = raw.startswith("-")
        line = raw[1:].strip() if optional else raw
        word, _, rest = line.partition(" ")
        if word == "echo":
            print(f"\n{BOLD}{BLUE}{expand(rest, lineno, display=True)}{RESET}", flush=True)
            continue
        idx += 1
        shown = expand(line, lineno, display=True)
        print(f"{DIM}[{idx}/{total}]{RESET} {YELLOW}$ {shown}{RESET}", flush=True)
        ok = True
        if word == "sleep":
            time.sleep(float(rest or 1))
        elif word == "pause":
            if not args.yes:
                input(f"{DIM}[ENTER para continuar]{RESET} ")
        elif line.startswith("!"):
            ok = subprocess.run(expand(line[1:].strip(), lineno, quote=True), shell=True, cwd=ROOT).returncode == 0
        else:
            tokens = [expand(t, lineno) for t in shlex.split(line)]
            if args.bucket != "default" and "--bucket" not in tokens:
                tokens = ["--bucket", args.bucket, *tokens]
            try:
                sub_args = parser.parse_args(tokens)
                if sub_args.cmd == "run":
                    sys.exit("no se puede usar 'run' dentro de un guion")
                sub_args.fn(sub_args)
            except SystemExit as exc:
                ok = exc.code in (None, 0)
                if isinstance(exc.code, str):
                    print(exc.code, file=sys.stderr)
        sys.stdout.flush()
        if not ok:
            if optional:
                print(f"{DIM}(falló, pero la línea empieza por '-': se continúa){RESET}")
                continue
            failures += 1
            print(f"{RED}✘ falló la acción {idx} ({args.script}:{lineno}){RESET}")
            if not args.keep_going:
                sys.exit(1)
    color = GREEN if failures == 0 else RED
    print(f"\n{color}{BOLD}■ {args.script}: {total - failures}/{total} acciones correctas"
          f" en {time.time() - started:.0f} s{RESET}")
    if failures:
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLI de P16", formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=__doc__)
    parser.add_argument("--bucket", default="default")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    p = sub.add_parser("watch"); p.add_argument("--interval", type=float, default=2)
    p.add_argument("--terminal", "-t", action="store_true", help="vista en la terminal en lugar del navegador")
    p.set_defaults(fn=cmd_watch)
    p = sub.add_parser("events"); p.add_argument("n", nargs="?", type=int, default=25); p.set_defaults(fn=cmd_events)
    p = sub.add_parser("put"); p.add_argument("file"); p.add_argument("key", nargs="?"); p.set_defaults(fn=cmd_put)
    p = sub.add_parser("get"); p.add_argument("key"); p.add_argument("out", nargs="?"); p.set_defaults(fn=cmd_get)
    p = sub.add_parser("head"); p.add_argument("key"); p.set_defaults(fn=cmd_head)
    p = sub.add_parser("ls"); p.add_argument("prefix", nargs="?", default=""); p.set_defaults(fn=cmd_ls)
    p = sub.add_parser("rm"); p.add_argument("key"); p.set_defaults(fn=cmd_rm)
    p = sub.add_parser("seed"); p.add_argument("n", type=int); p.add_argument("prefix", nargs="?", default="demo/")
    p.set_defaults(fn=cmd_seed)
    p = sub.add_parser("locate"); p.add_argument("key"); p.set_defaults(fn=cmd_locate)
    p = sub.add_parser("corrupt"); p.add_argument("key"); p.add_argument("node", nargs="?"); p.set_defaults(fn=cmd_corrupt)
    p = sub.add_parser("orphan"); p.add_argument("node"); p.set_defaults(fn=cmd_orphan)
    p = sub.add_parser("wipe"); p.add_argument("node"); p.set_defaults(fn=cmd_wipe)
    p = sub.add_parser("wait"); p.add_argument("what"); p.add_argument("state", nargs="?")
    p.add_argument("--timeout", type=float, default=180); p.set_defaults(fn=cmd_wait)
    sub.add_parser("reconcile").set_defaults(fn=cmd_simple_post("/cluster/reconcile"))
    sub.add_parser("scrub").set_defaults(fn=cmd_simple_post("/cluster/scrub"))
    p = sub.add_parser("drain"); p.add_argument("node")
    p.set_defaults(fn=lambda a: print(json.dumps(jcall("POST", f"/cluster/nodes/{a.node}/drain"), indent=2)))
    p = sub.add_parser("activate"); p.add_argument("node")
    p.set_defaults(fn=lambda a: print(json.dumps(jcall("POST", f"/cluster/nodes/{a.node}/activate"), indent=2)))
    p = sub.add_parser("weight"); p.add_argument("node"); p.add_argument("weight", type=float)
    p.set_defaults(fn=lambda a: print(json.dumps(
        jcall("PATCH", f"/cluster/nodes/{a.node}", {"weight": a.weight}), indent=2)))
    p = sub.add_parser("verify"); p.add_argument("file"); p.add_argument("key", nargs="?"); p.set_defaults(fn=cmd_verify)
    p = sub.add_parser("run", help="ejecutar un guion de acciones")
    p.add_argument("script"); p.add_argument("params", nargs="*")
    p.add_argument("--yes", "-y", action="store_true", help="no parar en las líneas 'pause'")
    p.add_argument("--keep-going", "-k", action="store_true", help="seguir aunque falle una acción")
    p.set_defaults(fn=cmd_run)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
