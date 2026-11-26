"""Pruebas del MetadataService dentro de Ray (se omiten si Ray no está instalado).

Evidencia para el mapa de dependencias (#38): reinicio real del actor por Ray,
descubrimiento por nombre, vida del actor ``detached`` más allá de su driver (#47)
y tipo de las excepciones a través de Ray.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from ray_proto import metadata_actor

ray = metadata_actor.ray

# Otro driver: se conecta al mismo clúster, crea un actor detached y otro que no lo
# es, confirma un objeto en cada uno y termina.
_OTHER_DRIVER = """
import hashlib, json, sys
import ray
from ray_proto import metadata_actor

address = sys.argv[1]
specs = {"detached": (sys.argv[2], sys.argv[3], True), "attached": (sys.argv[4], sys.argv[5], False)}
ray.init(address=address, log_to_driver=False)
out = {}
for label, (root, name, detached) in specs.items():
    actor = metadata_actor.get_or_create_metadata_service(
        root, name=name, detached=detached, snapshot_every=0)
    reserved = ray.get(actor.reserve.remote("b", "k"))
    obj, _ = ray.get(actor.commit_version.remote(
        object_id=reserved["id"], blob_id=metadata_actor.new_ulid(), size=1,
        checksum=hashlib.sha256(b"x").hexdigest(), content_type="text/plain",
        user_metadata={}, node_ids=["storage1", "storage2"]))
    out[label] = {"object_id": obj["id"], "pid": ray.get(actor.stats.remote())["pid"]}
print(json.dumps(out))
"""


@unittest.skipUnless(ray is not None, "Ray no está instalado")
class MetadataServiceRayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Los workers de Ray tienen que poder importar ray_proto. Se pasa por el entorno
        # heredado y NO con runtime_env: con runtime_env Ray arranca el worker con
        # `bash -c "exec <python>"` sin comillas y falla si la ruta tiene espacios
        # (como "Redes Distribuida"); ver docs/fase1/dependencias-notas.md.
        repo_root = str(Path(__file__).resolve().parents[2])
        cls._old_pythonpath = os.environ.get("PYTHONPATH")
        paths = [p for p in (cls._old_pythonpath or "").split(os.pathsep) if p]
        if repo_root not in paths:
            os.environ["PYTHONPATH"] = os.pathsep.join([repo_root, *paths])
        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()
        if cls._old_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = cls._old_pythonpath

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.name = f"p16-metadata-test-{uuid.uuid4().hex[:8]}"
        self.actor = metadata_actor.get_or_create_metadata_service(
            self.temp_dir.name, name=self.name, snapshot_every=0
        )

    def tearDown(self) -> None:
        ray.kill(self.actor, no_restart=True)
        self.temp_dir.cleanup()

    def commit(self, key: str, data: bytes) -> dict:
        reserved = ray.get(self.actor.reserve.remote("b", key))
        obj, _ = ray.get(self.actor.commit_version.remote(
            object_id=reserved["id"], blob_id=metadata_actor.new_ulid(), size=len(data),
            checksum=hashlib.sha256(data).hexdigest(), content_type="text/plain",
            user_metadata={}, node_ids=["storage1", "storage2"],
        ))
        return obj

    def wait_restarted(self, old_pid: int, timeout_s: float = 60) -> tuple[dict, list[str]]:
        """Espera a que responda un proceso distinto de ``old_pid``.

        ``ray.kill`` es asíncrono: la primera llamada puede contestarla todavía el
        proceso viejo. Devuelve las estadísticas y los errores vistos mientras tanto."""
        deadline = time.monotonic() + timeout_s
        errors: list[str] = []
        while True:
            try:
                stats = ray.get(self.actor.stats.remote(), timeout=5)
                if stats["pid"] != old_pid:
                    return stats, errors
            except ray.exceptions.RayError as exc:
                errors.append(type(exc).__name__)
            if time.monotonic() > deadline:
                self.fail(f"el actor no se reinició en {timeout_s} s (errores: {errors})")
            time.sleep(0.05)

    def test_killed_actor_is_restarted_by_ray_and_rebuilds_state_from_disk(self) -> None:
        self.commit("k", b"sobrevive al reinicio")
        before = ray.get(self.actor.dump_state.remote())
        pid_before = ray.get(self.actor.stats.remote())["pid"]

        ray.kill(self.actor, no_restart=False)
        started = time.monotonic()
        stats, errors = self.wait_restarted(pid_before)
        elapsed = time.monotonic() - started

        self.assertEqual(stats["recovery"]["replayed_records"], 2)  # reserve + commit
        self.assertEqual(ray.get(self.actor.dump_state.remote()), before)
        print(f"\n[evidencia] pid {pid_before} -> {stats['pid']}; {elapsed:.2f} s hasta responder; "
              f"errores durante el reinicio: {sorted(set(errors)) or 'ninguno'}; recovery={stats['recovery']}")

    def run_other_driver(self, detached: dict, attached: dict) -> dict:
        env = dict(os.environ)
        env.pop("RAY_ADDRESS", None)  # se conecta a la dirección explícita de este clúster
        done = subprocess.run(
            [sys.executable, "-c", _OTHER_DRIVER, ray.get_runtime_context().gcs_address,
             detached["root"], detached["name"], attached["root"], attached["name"]],
            capture_output=True, text=True, timeout=180, env=env, check=False,
        )
        self.assertEqual(done.returncode, 0, done.stderr[-3000:])
        return json.loads(done.stdout.strip().splitlines()[-1])

    def wait_until_dead(self, name: str, timeout_s: float = 60) -> str:
        """Espera a que el actor con nombre deje de existir y devuelve cómo se vio."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                handle = metadata_actor.get_metadata_service(name)
                ray.get(handle.stats.remote(), timeout=5)
            except ValueError:  # el nombre ya no está registrado
                return "ValueError (get_actor)"
            except ray.exceptions.ActorDiedError as exc:
                return type(exc).__name__
            except ray.exceptions.GetTimeoutError:
                pass
            time.sleep(0.1)
        self.fail(f"el actor {name} sigue vivo {timeout_s} s después de terminar su driver")

    def kill_by_name(self, name: str) -> None:
        try:
            ray.kill(metadata_actor.get_metadata_service(name), no_restart=True)
        except (ValueError, ray.exceptions.RayError):
            pass

    def test_only_the_detached_actor_survives_the_driver_that_created_it(self) -> None:
        roots = tempfile.TemporaryDirectory()
        self.addCleanup(roots.cleanup)
        tag = uuid.uuid4().hex[:8]
        detached = {"root": str(Path(roots.name) / "detached"), "name": f"p16-metadata-detached-{tag}"}
        attached = {"root": str(Path(roots.name) / "attached"), "name": f"p16-metadata-attached-{tag}"}

        created = self.run_other_driver(detached, attached)
        self.addCleanup(self.kill_by_name, detached["name"])

        # Su driver ya terminó: Ray mata el actor que no es detached y no lo reinicia,
        # aunque tenga max_restarts=-1 (no es un fallo, es el fin de su vida).
        started = time.monotonic()
        seen = self.wait_until_dead(attached["name"])
        elapsed = time.monotonic() - started

        # El detached sigue siendo el mismo proceso, con su estado, y se encuentra por nombre.
        survivor = metadata_actor.get_metadata_service(detached["name"])
        self.assertEqual(ray.get(survivor.stats.remote(), timeout=30)["pid"], created["detached"]["pid"])
        self.assertEqual(ray.get(survivor.find_active.remote("b", "k"), timeout=30)["id"],
                         created["detached"]["object_id"])

        # Lo que confirmó el actor muerto sigue en su disco: un actor nuevo lo reconstruye.
        recreated = metadata_actor.get_or_create_metadata_service(
            attached["root"], name=attached["name"], snapshot_every=0
        )
        self.addCleanup(self.kill_by_name, attached["name"])
        stats = ray.get(recreated.stats.remote(), timeout=60)
        self.assertEqual(ray.get(recreated.find_active.remote("b", "k"), timeout=30)["id"],
                         created["attached"]["object_id"])
        print(f"\n[evidencia] no detached: muerto {elapsed:.2f} s después de terminar su driver "
              f"({seen}); detached: mismo pid {created['detached']['pid']} y mismo objeto; "
              f"recreado sobre el mismo disco: replayed_records={stats['recovery']['replayed_records']}")

    def test_actor_is_discoverable_by_name(self) -> None:
        obj = self.commit("k", b"x")

        found = metadata_actor.get_metadata_service(self.name)

        self.assertEqual(ray.get(found.find_active.remote("b", "k"))["id"], obj["id"])

    def test_application_errors_keep_their_type_through_ray(self) -> None:
        with self.assertRaises(metadata_actor.CommitConflict):
            ray.get(self.actor.commit_version.remote(
                object_id="NO-EXISTE", blob_id=metadata_actor.new_ulid(), size=0,
                checksum=hashlib.sha256(b"").hexdigest(), content_type="text/plain",
                user_metadata={}, node_ids=["storage1"],
            ))


if __name__ == "__main__":
    unittest.main()
