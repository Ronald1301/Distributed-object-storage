from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ray_proto import storage_actor
from ray_proto.storage_actor import (
    ChecksumMismatch,
    InvalidBlobId,
    SidecarMissingError,
    _StorageNode,
)


class StorageNodeTests(unittest.TestCase):
    BLOB_ID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    def setUp(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self.temp_dir = tempfile.TemporaryDirectory(dir=repo_root)
        self.root = Path(self.temp_dir.name)
        self.directory_fsync_patcher = None
        if os.name == "nt":
            # The native BlobStore fsyncs directories on Linux; Windows cannot
            # open a directory with os.open, so exercise that code path elsewhere.
            self.directory_fsync_patcher = patch.object(storage_actor._blob_store, "_fsync_dir")
            self.directory_fsync_patcher.start()
        self.node = _StorageNode(self.root)

    def tearDown(self) -> None:
        if self.directory_fsync_patcher is not None:
            self.directory_fsync_patcher.stop()
        self.temp_dir.cleanup()

    def test_put_writes_blob_and_checksum_sidecar(self) -> None:
        data = b"contenido persistente"
        expected = hashlib.sha256(data).hexdigest()

        result = self.node.put(self.BLOB_ID, data, expected, "object-1")

        self.assertEqual(
            result,
            {"blob_id": self.BLOB_ID, "sha256": expected, "size": len(data)},
        )
        self.assertEqual(self.node.get(self.BLOB_ID), data)
        metadata = json.loads((self.root / "blobs" / f"{self.BLOB_ID}.meta.json").read_text())
        self.assertEqual(metadata["sha256"], expected)
        self.assertEqual(metadata["object_id"], "object-1")
        self.assertEqual(metadata["size"], len(data))

    def test_restart_reopens_confirmed_blob_from_disk(self) -> None:
        data = b"sobrevive al reinicio del actor"
        self.node.put(self.BLOB_ID, data, hashlib.sha256(data).hexdigest())

        restarted_node = _StorageNode(self.root)

        self.assertEqual(restarted_node.get(self.BLOB_ID), data)

    def test_get_rejects_corrupted_blob(self) -> None:
        data = b"original"
        self.node.put(self.BLOB_ID, data, hashlib.sha256(data).hexdigest())
        (self.root / "blobs" / self.BLOB_ID).write_bytes(b"corrupto")

        with self.assertRaises(ChecksumMismatch):
            self.node.get(self.BLOB_ID)

    def test_get_fails_closed_when_sidecar_is_missing(self) -> None:
        data = b"sin sidecar"
        self.node.put(self.BLOB_ID, data, hashlib.sha256(data).hexdigest())
        (self.root / "blobs" / f"{self.BLOB_ID}.meta.json").unlink()

        with self.assertRaises(SidecarMissingError):
            self.node.get(self.BLOB_ID)

    def test_checksum_mismatch_does_not_publish_partial_blob(self) -> None:
        with self.assertRaises(ChecksumMismatch):
            self.node.put(self.BLOB_ID, b"bytes", "0" * 64)

        self.assertFalse((self.root / "blobs" / self.BLOB_ID).exists())
        self.assertFalse((self.root / "blobs" / f"{self.BLOB_ID}.meta.json").exists())
        self.assertEqual(list((self.root / "tmp").iterdir()), [])

    def test_invalid_blob_id_is_rejected(self) -> None:
        with self.assertRaises(InvalidBlobId):
            self.node.put("../outside", b"bytes", hashlib.sha256(b"bytes").hexdigest())

    def test_delete_removes_blob_and_sidecar(self) -> None:
        data = b"borrar"
        self.node.put(self.BLOB_ID, data, hashlib.sha256(data).hexdigest())

        self.assertTrue(self.node.delete(self.BLOB_ID))
        self.assertIsNone(self.node.get(self.BLOB_ID))
        self.assertFalse((self.root / "blobs" / f"{self.BLOB_ID}.meta.json").exists())


if __name__ == "__main__":
    unittest.main()
