"""Pruebas del MetadataService dentro de Ray (se omiten si Ray no está instalado).

Evidencia para el mapa de dependencias (#38): reinicio real del actor por Ray,
descubrimiento por nombre y tipo de las excepciones a través de Ray.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from ray_proto import metadata_actor

ray = metadata_actor.ray


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
