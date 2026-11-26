"""Pruebas del MetadataService sin Ray: semántica (paridad con metadata.py) y recuperación."""
from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ray_proto import metadata_log, storage_actor
from ray_proto.metadata_actor import (
    CommitConflict,
    PreconditionFailed,
    _MetadataService,
    new_ulid,
)
from ray_proto.metadata_log import CorruptLogError, encode_line

BUCKET = "b"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeClock:
    def __init__(self, start: float = 1_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class MetadataServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.directory_fsync_patcher = None
        if os.name == "nt":
            # Igual que en test_storage_actor.py: Windows no permite os.open sobre un directorio.
            self.directory_fsync_patcher = patch.object(storage_actor._blob_store, "_fsync_dir")
            self.directory_fsync_patcher.start()
        self.clock = FakeClock()
        self._opened: list[_MetadataService] = []
        self.svc = self.open()

    def tearDown(self) -> None:
        for svc in self._opened:
            svc.close()
        if self.directory_fsync_patcher is not None:
            self.directory_fsync_patcher.stop()
        self.temp_dir.cleanup()

    def open(self, **kwargs) -> _MetadataService:
        svc = _MetadataService(self.root, clock=self.clock, **kwargs)
        self._opened.append(svc)
        return svc

    def restart(self, **kwargs) -> _MetadataService:
        """Simula la muerte del proceso del actor y su reinicio sobre el mismo disco."""
        self.svc.close()
        self.svc = self.open(**kwargs)
        return self.svc

    def put(self, key: str, data: bytes | None = None, nodes=("storage1", "storage2"), **kwargs):
        data = data if data is not None else f"contenido de {key} @ {self.clock.now}".encode()
        reserved = self.svc.reserve(BUCKET, key)
        return self.svc.commit_version(
            object_id=reserved["id"], blob_id=new_ulid(), size=len(data), checksum=sha256(data),
            content_type="text/plain", user_metadata={"autor": "test"}, node_ids=list(nodes), **kwargs,
        )

    def workload(self) -> None:
        """Toca todas las operaciones que escriben en el WAL."""
        svc = self.svc
        svc.upsert_node("storage1", "actor:storage1")
        svc.upsert_node("storage2", "actor:storage2")
        svc.upsert_node("storage3", "actor:storage3", weight=2)
        self.put("a.txt", request_id="req-a")
        self.clock.advance(1)
        self.put("a.txt")  # sobrescritura: encola las réplicas de v1
        b, _ = self.put("b.txt", nodes=("storage2", "storage3"))
        svc.set_replica_status(b["id"], "storage2", b["blob_id"], "CORRUPT")
        svc.add_replica(b["id"], "storage1", b["blob_id"])
        svc.remove_replica(b["id"], "storage2", b["blob_id"], "corrupt", 2)
        svc.mark_verified("storage1", [b["id"]])
        self.put("c.txt")
        svc.mark_deleted(BUCKET, "c.txt")
        pending = svc.pending_deletions(0)
        svc.fail_deletion(pending[0]["blob_id"], pending[0]["node_id"], "timeout")
        svc.complete_deletion(pending[1]["blob_id"], pending[1]["node_id"])
        svc.reserve(BUCKET, "pendiente")
        svc.set_node_membership("storage3", "DRAINING")
        self.clock.advance(5)


class SemanticsTests(MetadataServiceTestCase):
    def test_reserve_returns_the_same_pending_id_until_commit(self) -> None:
        first = self.svc.reserve(BUCKET, "k")
        second = self.svc.reserve(BUCKET, "k")

        self.assertEqual(first["id"], second["id"])
        self.assertEqual((first["status"], first["version"]), ("PENDING", 0))
        self.assertIsNone(self.svc.find_active(BUCKET, "k"))
        self.assertIsNotNone(self.svc.find_live(BUCKET, "k"))
        self.assertEqual(self.svc.stats()["last_seq"], 1)  # la segunda reserva no escribe

    def test_commit_publishes_version_with_its_replicas(self) -> None:
        obj, created = self.put("k", b"hola")

        self.assertTrue(created)
        self.assertEqual((obj["status"], obj["version"], obj["checksum"]), ("ACTIVE", 1, sha256(b"hola")))
        found = self.svc.find_active_with_replicas(BUCKET, "k")
        self.assertEqual(
            found["replicas"],
            [{"node_id": "storage1", "blob_id": obj["blob_id"], "status": "OK"},
             {"node_id": "storage2", "blob_id": obj["blob_id"], "status": "OK"}],
        )
        self.assertEqual([o["key"] for o in self.svc.list_objects(BUCKET, "", None, 10)], ["k"])
        self.assertEqual(self.svc.list_objects(BUCKET, "", None, 10)[0]["replicas"], ["storage1", "storage2"])
        self.assertEqual(self.svc.list_buckets(), [{"bucket": BUCKET, "objects": 1, "bytes": 4}])
        self.assertEqual(self.svc.count_active(), 1)

    def test_overwrite_queues_previous_replicas_for_physical_deletion(self) -> None:
        v1, _ = self.put("k")
        v2, created = self.put("k")

        self.assertFalse(created)
        self.assertEqual((v2["id"], v2["version"]), (v1["id"], 2))
        queued = self.svc.pending_deletions_detail()
        self.assertEqual({(d["blob_id"], d["node_id"]) for d in queued},
                         {(v1["blob_id"], "storage1"), (v1["blob_id"], "storage2")})
        self.assertEqual({d["reason"] for d in queued}, {"overwritten"})
        self.assertEqual({d["label"] for d in queued}, {f"{BUCKET}/k v1"})
        self.assertEqual({r["blob_id"] for r in self.svc.replicas_of(v1["id"])}, {v2["blob_id"]})

    def test_conditional_writes(self) -> None:
        obj, _ = self.put("k")

        with self.assertRaises(PreconditionFailed):
            self.put("k", if_none_match="*")
        with self.assertRaises(PreconditionFailed):
            self.put("k", if_match='"otro-etag"')
        v2, _ = self.put("k", if_match=f'"{obj["checksum"]}"')
        self.assertEqual(v2["version"], 2)
        with self.assertRaises(PreconditionFailed):
            self.put("nuevo", if_match="*")  # If-Match sobre un objeto que no existe

    def test_commit_after_concurrent_delete_conflicts(self) -> None:
        self.put("k")
        reserved = self.svc.reserve(BUCKET, "k")
        self.svc.mark_deleted(BUCKET, "k")

        with self.assertRaises(CommitConflict):
            self.svc.commit_version(
                object_id=reserved["id"], blob_id=new_ulid(), size=1, checksum=sha256(b"x"),
                content_type="text/plain", user_metadata={}, node_ids=["storage1"],
            )

    def test_retry_with_same_request_id_does_not_create_a_second_version(self) -> None:
        first, created = self.put("k", b"v1", request_id="put-1")
        seq = self.svc.stats()["last_seq"]

        retry, retry_created = self.put("k", b"v1", request_id="put-1")  # blob_id nuevo

        self.assertTrue(created and retry_created)
        self.assertEqual((retry["version"], retry["blob_id"]), (1, first["blob_id"]))
        self.assertEqual(self.svc.pending_deletions_detail(), [])
        self.assertEqual(self.svc.stats()["last_seq"], seq)  # el commit repetido no escribe
        with self.assertRaises(CommitConflict):
            self.put("otra-clave", request_id="put-1")

    def test_delete_then_recreate_gets_a_new_stable_id(self) -> None:
        obj, _ = self.put("k")

        deleted = self.svc.mark_deleted(BUCKET, "k")

        self.assertEqual(deleted["queued_deletions"], ["storage1", "storage2"])
        self.assertIsNone(self.svc.find_active(BUCKET, "k"))
        self.assertEqual(self.svc.get_object(obj["id"])["status"], "DELETED")
        self.assertEqual(self.svc.replicas_of(obj["id"]), [])
        self.assertIsNone(self.svc.mark_deleted(BUCKET, "k"))
        self.assertNotEqual(self.svc.reserve(BUCKET, "k")["id"], obj["id"])

    def test_two_copies_on_the_same_node_are_rejected_before_logging(self) -> None:
        reserved = self.svc.reserve(BUCKET, "k")
        seq = self.svc.stats()["last_seq"]

        with self.assertRaises(ValueError):
            self.svc.commit_version(
                object_id=reserved["id"], blob_id=new_ulid(), size=1, checksum=sha256(b"x"),
                content_type="text/plain", user_metadata={}, node_ids=["storage1", "storage1"],
            )
        self.assertEqual(self.svc.stats()["last_seq"], seq)

    def test_replica_guards_match_the_native_repository(self) -> None:
        obj, _ = self.put("k")
        oid, blob = obj["id"], obj["blob_id"]

        self.assertFalse(self.svc.add_replica(oid, "storage3", new_ulid()))  # versión antigua
        self.assertTrue(self.svc.add_replica(oid, "storage3", blob))
        self.assertTrue(self.svc.remove_replica(oid, "storage1", blob, "rebalance", 2))
        self.assertFalse(self.svc.remove_replica(oid, "storage2", blob, "rebalance", 2))  # quedaría 1
        self.assertTrue(self.svc.set_replica_status(oid, "storage3", blob, "CORRUPT"))
        self.assertFalse(self.svc.set_replica_status(oid, "storage3", blob, "CORRUPT"))
        self.assertFalse(self.svc.remove_replica(oid, "storage2", blob, "x", 1))  # la CORRUPT no cuenta
        self.assertEqual(self.svc.count_replicas_on_node("storage3"), 1)
        self.assertEqual([r["object_id"] for r in self.svc.replicas_on_node("storage2")], [oid])
        self.assertTrue(self.svc.is_referenced(blob, "storage2"))
        self.assertFalse(self.svc.is_referenced(blob, "storage1"))

    def test_add_replica_cancels_a_pending_deletion_of_the_same_copy(self) -> None:
        obj, _ = self.put("k", nodes=("storage1", "storage2", "storage3"))
        self.svc.remove_replica(obj["id"], "storage3", obj["blob_id"], "rebalance", 2)
        self.assertIn(obj["blob_id"], self.svc.pending_deletion_blob_ids("storage3"))

        self.svc.add_replica(obj["id"], "storage3", obj["blob_id"])

        self.assertNotIn(obj["blob_id"], self.svc.pending_deletion_blob_ids("storage3"))

    def test_deletion_queue_lifecycle(self) -> None:
        obj, _ = self.put("k")
        self.svc.mark_deleted(BUCKET, "k")
        blob = obj["blob_id"]

        self.assertEqual(self.svc.pending_deletions(min_age_s=10), [])
        self.clock.advance(10)
        self.assertEqual(len(self.svc.pending_deletions(min_age_s=10)), 2)
        self.svc.fail_deletion(blob, "storage1", "x" * 600)
        detail = {d["node_id"]: d for d in self.svc.pending_deletions_detail()}
        self.assertEqual(detail["storage1"]["attempts"], 1)
        self.assertEqual(self.svc.dump_state()["deletions"][0]["last_error"], "x" * 500)
        self.svc.complete_deletion(blob, "storage1")
        self.assertEqual(self.svc.deletion_backlog(), [{"node_id": "storage2", "pending": 1}])
        self.svc.drop_deletions_for_node("storage2")
        self.assertEqual(self.svc.deletion_backlog(), [])

    def test_cleanup_removes_only_stale_reservations(self) -> None:
        pending = self.svc.reserve(BUCKET, "abandonado")
        committed, _ = self.put("confirmado")
        self.clock.advance(100)

        self.assertEqual(self.svc.cleanup_stale_pending(60), 1)
        self.assertIsNone(self.svc.get_object(pending["id"]))
        self.assertIsNotNone(self.svc.get_object(committed["id"]))
        self.assertNotEqual(self.svc.reserve(BUCKET, "abandonado")["id"], pending["id"])
        self.assertEqual(self.svc.cleanup_stale_pending(60), 0)

    def test_nodes_keep_membership_and_weight_but_not_health(self) -> None:
        node = self.svc.upsert_node("storage1", "actor:storage1")
        seq = self.svc.stats()["last_seq"]
        self.svc.upsert_node("storage1", "actor:storage1")  # sin cambios: no escribe
        self.assertEqual(self.svc.stats()["last_seq"], seq)

        self.clock.advance(5)
        updated = self.svc.set_node_weight("storage1", 3)
        self.svc.set_node_membership("storage1", "DRAINING")

        self.assertEqual(updated["joined_at"], node["joined_at"])
        self.assertEqual(self.svc.get_node("storage1")["membership"], "DRAINING")
        self.assertNotIn("health", self.svc.get_node("storage1"))
        with self.assertRaises(ValueError):
            self.svc.set_node_membership("storage1", "ROTO")
        with self.assertRaises(ValueError):
            self.svc.set_node_weight("storage1", 0)
        with self.assertRaises(KeyError):
            self.svc.set_node_weight("storage9", 1)


class RecoveryTests(MetadataServiceTestCase):
    def test_restart_rebuilds_identical_state_from_the_wal(self) -> None:
        self.workload()
        before = self.svc.dump_state()
        seq = self.svc.stats()["last_seq"]

        restarted = self.restart()

        self.assertEqual(restarted.dump_state(), before)
        recovery = restarted.stats()["recovery"]
        self.assertIsNone(recovery["snapshot_seq"])
        self.assertEqual(recovery["replayed_records"], seq)
        self.assertEqual(restarted.stats()["last_seq"], seq)

    def test_restart_from_snapshot_plus_newer_wal_records(self) -> None:
        self.workload()
        snapshot_seq = self.svc.snapshot()["last_seq"]
        self.put("despues-del-snapshot.txt")
        before = self.svc.dump_state()

        restarted = self.restart()

        self.assertEqual(restarted.dump_state(), before)
        self.assertEqual(restarted.stats()["recovery"]["snapshot_seq"], snapshot_seq)
        self.assertEqual(restarted.stats()["recovery"]["replayed_records"], 2)  # reserve + commit

    def test_automatic_snapshot_empties_the_wal(self) -> None:
        self.svc = self.restart(snapshot_every=3)
        self.put("a")  # 2 registros
        self.put("b")  # 2 más: snapshot al llegar a 3

        self.assertTrue((self.root / "snapshot.json").exists())
        self.assertEqual(self.svc.stats()["records_since_snapshot"], 1)
        before = self.svc.dump_state()
        self.assertEqual(self.restart(snapshot_every=3).dump_state(), before)

    def test_torn_last_record_is_discarded(self) -> None:
        self.workload()
        before = self.svc.dump_state()
        wal = self.root / "wal.log"
        with open(wal, "ab") as fh:
            fh.write(b'0badc0de {"seq": 999, "op": "reserve"')  # murió a mitad de escribir

        restarted = self.restart()

        self.assertEqual(restarted.dump_state(), before)
        self.assertGreater(restarted.stats()["recovery"]["discarded_tail_bytes"], 0)
        self.put("tras-el-corte")  # el WAL truncado admite registros nuevos
        after = restarted.dump_state()
        self.assertEqual(self.restart().dump_state(), after)

    def test_complete_last_line_with_bad_crc_is_discarded(self) -> None:
        self.put("k")
        before = self.svc.dump_state()
        line = encode_line({"seq": self.svc.stats()["last_seq"] + 1, "op": "reserve", "args": {}})
        with open(self.root / "wal.log", "ab") as fh:
            fh.write(b"00000000" + line[8:])

        self.assertEqual(self.restart().dump_state(), before)

    def test_corruption_before_valid_records_fails_closed(self) -> None:
        self.workload()
        wal = self.root / "wal.log"
        lines = wal.read_bytes().splitlines(keepends=True)
        lines[1] = lines[1].replace(b"storage", b"st0rage", 1)  # bit rot, CRC ya no cuadra
        wal.write_bytes(b"".join(lines))

        self.svc.close()
        with self.assertRaises(CorruptLogError):
            self.open()

    def test_unknown_operation_fails_closed(self) -> None:
        self.put("k")
        line = encode_line({"seq": self.svc.stats()["last_seq"] + 1, "op": "drop_table", "args": {}})
        with open(self.root / "wal.log", "ab") as fh:
            fh.write(line)

        self.svc.close()
        with self.assertRaises(CorruptLogError):
            self.open()

    def test_crash_between_snapshot_and_wal_reset_does_not_apply_twice(self) -> None:
        self.workload()
        old_wal = (self.root / "wal.log").read_bytes()
        self.svc.snapshot()
        (self.root / "wal.log").write_bytes(old_wal)  # el WAL viejo no llegó a sustituirse
        before = self.svc.dump_state()

        restarted = self.restart()

        self.assertEqual(restarted.dump_state(), before)
        self.assertGreater(restarted.stats()["recovery"]["skipped_records"], 0)
        self.put("nuevo")  # seq continúa tras el snapshot
        after = restarted.dump_state()
        self.assertEqual(self.restart().dump_state(), after)

    def test_failed_fsync_changes_neither_memory_nor_disk(self) -> None:
        self.workload()
        before = self.svc.dump_state()
        seq = self.svc.stats()["last_seq"]
        real_fsync = os.fsync
        calls = {"n": 0}

        def flaky_fsync(fd: int) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("disco lleno")
            real_fsync(fd)

        with patch.object(metadata_log.os, "fsync", flaky_fsync), self.assertRaises(OSError):
            self.svc.reserve(BUCKET, "no-confirmado")

        self.assertEqual(self.svc.dump_state(), before)
        self.assertEqual(self.svc.stats()["last_seq"], seq)
        self.assertEqual(self.restart().dump_state(), before)  # tampoco quedó en el WAL
        self.svc.reserve(BUCKET, "si-confirmado")
        self.assertEqual(self.svc.stats()["last_seq"], seq + 1)

    def test_request_ids_survive_a_restart(self) -> None:
        first, _ = self.put("k", request_id="put-1")

        self.restart()
        retry, _ = self.put("k", request_id="put-1")

        self.assertEqual((retry["version"], retry["blob_id"]), (1, first["blob_id"]))

    def test_corrupt_snapshot_fails_closed(self) -> None:
        self.workload()
        self.svc.snapshot()
        snapshot = self.root / "snapshot.json"
        snapshot.write_bytes(snapshot.read_bytes().replace(b"storage", b"st0rage", 1))

        self.svc.close()
        with self.assertRaises(CorruptLogError):
            self.open()


if __name__ == "__main__":
    unittest.main()
