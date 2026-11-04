"""Filesystem-backed raw artifact interface."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

from pncp_etl.contracts import ArtifactRef, QueryWindow, SafetyLimits, SourcePage
from pncp_etl.pncp.errors import ContractError


class ArtifactRepository:
    """Publish immutable stage artifacts below one configured root."""

    def __init__(self, root: Path, *, now: Callable[[], datetime]) -> None:
        self.root = root
        self.now = now

    def publish_raw(
        self,
        *,
        execution_id: str,
        window: QueryWindow,
        limits: SafetyLimits,
        pages: Iterable[SourcePage],
        source: str = "vigencia",
    ) -> ArtifactRef:
        """Stream source pages to an immutable raw JSONL artifact."""
        if not execution_id.strip():
            raise ValueError("execution_id must not be empty")
        if source not in {"vigencia", "atualizacao"}:
            raise ValueError("source must be vigencia or atualizacao")
        stage_dir = self._stage_dir(execution_id, "extract")
        stage_dir.mkdir(parents=True, exist_ok=True)
        raw_path = stage_dir / "raw.jsonl"
        raw_part = stage_dir / "raw.jsonl.part"
        manifest_path = stage_dir / "manifest.json"
        manifest_part = stage_dir / "manifest.json.part"
        if manifest_path.exists():
            return self._load_complete(
                manifest_path=manifest_path,
                execution_id=execution_id,
                window=window,
                source=source,
            )
        raw_part.unlink(missing_ok=True)
        manifest_part.unlink(missing_ok=True)

        digest = hashlib.sha256()
        byte_count = 0
        record_count = 0
        page_count = 0
        started_at = self.now()
        with raw_part.open("wb") as raw_file:
            try:
                for page in pages:
                    if not isinstance(page, SourcePage):
                        raise ContractError("raw publisher received an invalid page")
                    page_count += 1
                    for record_index, payload in enumerate(page.records):
                        line = self._raw_line(
                            execution_id=execution_id,
                            page=page,
                            record_index=record_index,
                            payload=payload,
                        )
                        if byte_count + len(line) > limits.max_artifact_bytes:
                            raise ContractError("max_artifact_bytes exceeded")
                        raw_file.write(line)
                        digest.update(line)
                        byte_count += len(line)
                        record_count += 1
            finally:
                self._sync_file(raw_file)

        os.replace(raw_part, raw_path)
        self._sync_directory(stage_dir)
        completed_at = self.now()
        raw_relative = self._relative(raw_path)
        manifest = {
            "schema_version": 1,
            "artifact_type": "raw",
            "stage": "extract",
            "complete": True,
            "execution_id": execution_id,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "source": source,
            "window": self._window_payload(window),
            "record_count": record_count,
            "page_count": page_count,
            "artifact_checksum": digest.hexdigest(),
            "files": [
                {
                    "path": raw_relative,
                    "sha256": digest.hexdigest(),
                    "bytes": byte_count,
                }
            ],
        }
        self._write_manifest(manifest_part, manifest)
        os.replace(manifest_part, manifest_path)
        self._sync_directory(stage_dir)
        return ArtifactRef(
            execution_id=execution_id,
            stage="extract",
            manifest_path=self._relative(manifest_path),
            record_count=record_count,
            checksum=digest.hexdigest(),
        )

    def _load_complete(
        self,
        *,
        manifest_path: Path,
        execution_id: str,
        window: QueryWindow,
        source: str,
    ) -> ArtifactRef:
        """Verify and reuse an artifact committed before a producer retry."""
        try:
            manifest: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ContractError("existing raw manifest is unreadable") from error
        if not isinstance(manifest, dict) or manifest.get("complete") is not True:
            raise ContractError("existing raw manifest is not complete")
        if manifest.get("execution_id") != execution_id:
            raise ContractError("existing raw manifest belongs to another execution")
        if manifest.get("stage") != "extract" or manifest.get("artifact_type") != "raw":
            raise ContractError("existing manifest has an incompatible artifact type")
        if manifest.get("window") != self._window_payload(window):
            raise ContractError("existing raw manifest has a different query window")
        if manifest.get("source") != source:
            raise ContractError("existing raw manifest has a different query source")

        files = manifest.get("files")
        if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
            raise ContractError("existing raw manifest must reference exactly one file")
        file_entry = files[0]
        relative_path = file_entry.get("path")
        expected_checksum = file_entry.get("sha256")
        expected_bytes = file_entry.get("bytes")
        if not isinstance(relative_path, str) or not isinstance(expected_checksum, str):
            raise ContractError("existing raw manifest has invalid file metadata")
        if isinstance(expected_bytes, bool) or not isinstance(expected_bytes, int):
            raise ContractError("existing raw manifest has invalid byte count")
        raw_path = self._safe_artifact_path(relative_path)
        try:
            actual_bytes = raw_path.stat().st_size
        except OSError as error:
            raise ContractError("existing raw artifact is missing") from error
        if actual_bytes != expected_bytes:
            raise ContractError("existing raw artifact byte count does not match manifest")
        actual_checksum = self._file_checksum(raw_path)
        if actual_checksum != expected_checksum:
            raise ContractError("existing raw artifact checksum does not match manifest")

        record_count = manifest.get("record_count")
        if isinstance(record_count, bool) or not isinstance(record_count, int) or record_count < 0:
            raise ContractError("existing raw manifest has invalid record_count")
        return ArtifactRef(
            execution_id=execution_id,
            stage="extract",
            manifest_path=self._relative(manifest_path),
            record_count=record_count,
            checksum=actual_checksum,
        )

    def read_manifest(self, ref: ArtifactRef) -> dict[str, Any]:
        """Read a complete manifest and verify every referenced file."""
        expected_path = self._stage_dir(ref.execution_id, ref.stage) / "manifest.json"
        manifest_path = self._safe_artifact_path(ref.manifest_path)
        if manifest_path != expected_path:
            raise ContractError("manifest path does not match execution and stage")
        manifest = self._read_manifest_path(manifest_path)
        if manifest.get("execution_id") != ref.execution_id or manifest.get("stage") != ref.stage:
            raise ContractError("manifest identity does not match artifact reference")
        if manifest.get("complete") is not True:
            raise ContractError("artifact manifest is not complete")
        record_count = manifest.get("record_count")
        if record_count != ref.record_count:
            raise ContractError("artifact record count does not match its reference")
        file_checksums = self._verify_manifest_files(manifest, manifest_path.parent)
        artifact_checksum = manifest.get("artifact_checksum")
        if not isinstance(artifact_checksum, str):
            artifact_checksum = self._combined_checksum(file_checksums)
        if artifact_checksum != ref.checksum:
            raise ContractError("artifact checksum does not match its reference")
        return manifest

    def iter_jsonl(self, ref: ArtifactRef, filename: str) -> Iterator[dict[str, Any]]:
        """Yield verified JSON objects from one named stage file."""
        manifest = self.read_manifest(ref)
        entries = [entry for entry in manifest["files"] if entry.get("name") == filename]
        if not entries:
            entries = [
                entry
                for entry in manifest["files"]
                if Path(str(entry.get("path", ""))).name == filename
            ]
        if len(entries) != 1:
            raise ContractError(f"manifest must reference exactly one {filename}")
        path = self._safe_artifact_path(entries[0]["path"])
        try:
            with path.open("rb") as jsonl_file:
                for line_number, line in enumerate(jsonl_file, start=1):
                    try:
                        value: Any = json.loads(line)
                    except (UnicodeError, json.JSONDecodeError) as error:
                        raise ContractError(
                            f"{filename} contains invalid JSON at line {line_number}"
                        ) from error
                    if not isinstance(value, dict):
                        raise ContractError(
                            f"{filename} line {line_number} must be a JSON object"
                        )
                    yield value
        except OSError as error:
            raise ContractError(f"cannot read verified artifact file {filename}") from error

    def reuse_stage(
        self,
        *,
        execution_id: str,
        stage: str,
        input_ref: ArtifactRef,
    ) -> ArtifactRef | None:
        """Verify and reuse a child stage committed before its task retry."""
        manifest_path = self._stage_dir(execution_id, stage) / "manifest.json"
        if not manifest_path.exists():
            return None
        manifest = self._read_manifest_path(manifest_path)
        expected_input = self._input_payload(input_ref)
        if manifest.get("input") != expected_input:
            raise ContractError("existing stage manifest has a different input artifact")
        record_count = manifest.get("record_count")
        checksum = manifest.get("artifact_checksum")
        if isinstance(record_count, bool) or not isinstance(record_count, int):
            raise ContractError("existing stage manifest has invalid record_count")
        if not isinstance(checksum, str):
            raise ContractError("existing stage manifest has no artifact checksum")
        ref = ArtifactRef(
            execution_id=execution_id,
            stage=stage,
            manifest_path=self._relative(manifest_path),
            record_count=record_count,
            checksum=checksum,
        )
        self.read_manifest(ref)
        return ref

    def stage_writer(
        self,
        *,
        execution_id: str,
        stage: str,
        filenames: tuple[str, ...],
        input_ref: ArtifactRef,
        max_artifact_bytes: int,
    ) -> StageArtifactWriter:
        """Create an atomic multi-file writer for one producer stage."""
        return StageArtifactWriter(
            repository=self,
            execution_id=execution_id,
            stage=stage,
            filenames=filenames,
            input_ref=input_ref,
            max_artifact_bytes=max_artifact_bytes,
        )

    def _stage_dir(self, execution_id: str, stage: str) -> Path:
        if not stage or "/" in stage or "\\" in stage or stage in {".", ".."}:
            raise ContractError("stage name is unsafe")
        execution_key = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()[:24]
        return self.root / "executions" / execution_key / stage

    @staticmethod
    def _window_payload(window: QueryWindow) -> dict[str, str | int | None]:
        return {
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "cnpj": window.cnpj,
            "administrative_unit": window.administrative_unit,
            "user_id": window.user_id,
        }

    def _safe_artifact_path(self, relative_path: str) -> Path:
        candidate = self.root / relative_path
        try:
            candidate.resolve().relative_to(self.root.resolve())
        except (OSError, ValueError) as error:
            raise ContractError("artifact path escapes the configured root") from error
        return candidate

    @staticmethod
    def _read_manifest_path(manifest_path: Path) -> dict[str, Any]:
        try:
            value: Any = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ContractError("artifact manifest is unreadable") from error
        if not isinstance(value, dict):
            raise ContractError("artifact manifest must be a JSON object")
        return value

    def _verify_manifest_files(
        self,
        manifest: dict[str, Any],
        expected_directory: Path,
    ) -> list[tuple[str, str, int]]:
        files = manifest.get("files")
        if not isinstance(files, list) or not files:
            raise ContractError("artifact manifest has no files")
        verified: list[tuple[str, str, int]] = []
        seen_names: set[str] = set()
        for entry in files:
            if not isinstance(entry, dict):
                raise ContractError("artifact manifest file entry is invalid")
            relative_path = entry.get("path")
            checksum = entry.get("sha256")
            byte_count = entry.get("bytes")
            name = entry.get("name")
            if name is None and isinstance(relative_path, str):
                name = Path(relative_path).name
            if not isinstance(name, str) or name in seen_names:
                raise ContractError("artifact manifest file name is invalid or duplicated")
            if not isinstance(relative_path, str) or not isinstance(checksum, str):
                raise ContractError("artifact manifest file metadata is invalid")
            if isinstance(byte_count, bool) or not isinstance(byte_count, int):
                raise ContractError("artifact manifest byte count is invalid")
            path = self._safe_artifact_path(relative_path)
            if path.parent != expected_directory:
                raise ContractError("artifact file is outside its stage directory")
            try:
                actual_bytes = path.stat().st_size
            except OSError as error:
                raise ContractError("artifact file is missing") from error
            if actual_bytes != byte_count:
                raise ContractError("artifact file byte count does not match manifest")
            if self._file_checksum(path) != checksum:
                raise ContractError("artifact file checksum does not match manifest")
            seen_names.add(name)
            verified.append((name, checksum, byte_count))
        return verified

    @staticmethod
    def _combined_checksum(files: Iterable[tuple[str, str, int]]) -> str:
        digest = hashlib.sha256()
        for name, checksum, byte_count in sorted(files):
            digest.update(f"{name}:{checksum}:{byte_count}\n".encode())
        return digest.hexdigest()

    @staticmethod
    def _input_payload(ref: ArtifactRef) -> dict[str, str | int]:
        return {
            "execution_id": ref.execution_id,
            "stage": ref.stage,
            "manifest_path": ref.manifest_path,
            "record_count": ref.record_count,
            "checksum": ref.checksum,
        }

    @staticmethod
    def _file_checksum(path: Path) -> str:
        digest = hashlib.sha256()
        try:
            with path.open("rb") as artifact_file:
                for chunk in iter(lambda: artifact_file.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError as error:
            raise ContractError("existing raw artifact cannot be read") from error
        return digest.hexdigest()

    def _raw_line(
        self,
        *,
        execution_id: str,
        page: SourcePage,
        record_index: int,
        payload: dict[str, Any],
    ) -> bytes:
        wrapper = {
            "execution_id": execution_id,
            "page_number": page.number,
            "record_index": record_index,
            "collected_at": self.now().isoformat(),
            "payload": payload,
        }
        return (
            json.dumps(wrapper, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")

    def _relative(self, path: Path) -> str:
        return path.relative_to(self.root).as_posix()

    @staticmethod
    def _sync_file(file: BinaryIO) -> None:
        file.flush()
        os.fsync(file.fileno())

    @staticmethod
    def _sync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
        with path.open("wb") as manifest_file:
            content = (
                json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                + "\n"
            ).encode("utf-8")
            manifest_file.write(content)
            ArtifactRepository._sync_file(manifest_file)


class StageArtifactWriter:
    """Write several stage files and publish their manifest last."""

    def __init__(
        self,
        *,
        repository: ArtifactRepository,
        execution_id: str,
        stage: str,
        filenames: tuple[str, ...],
        input_ref: ArtifactRef,
        max_artifact_bytes: int,
    ) -> None:
        if not filenames or len(set(filenames)) != len(filenames):
            raise ValueError("stage writer requires unique output filenames")
        if max_artifact_bytes <= 0:
            raise ValueError("max_artifact_bytes must be positive")
        self._repository = repository
        self._execution_id = execution_id
        self._stage = stage
        self._input_ref = input_ref
        self._max_bytes = max_artifact_bytes
        self._stage_dir = repository._stage_dir(execution_id, stage)
        self._stage_dir.mkdir(parents=True, exist_ok=True)
        self._files: dict[str, BinaryIO] = {}
        self._digests: dict[str, Any] = {}
        self._byte_counts: dict[str, int] = {}
        self._external_parts: set[str] = set()
        self._total_bytes = 0
        self._committed = False
        self._started_at = repository.now()
        for filename in filenames:
            self._validate_filename(filename)
            part_path = self.part_path(filename)
            part_path.unlink(missing_ok=True)
            self._files[filename] = part_path.open("wb")
            self._digests[filename] = hashlib.sha256()
            self._byte_counts[filename] = 0
        (self._stage_dir / "manifest.json.part").unlink(missing_ok=True)

    def __enter__(self) -> StageArtifactWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        del exc_type, exc_value, traceback
        self._close_outputs()

    def write(self, filename: str, value: Mapping[str, Any]) -> None:
        """Append one canonical JSON object to a configured output file."""
        if self._committed:
            raise ContractError("cannot write after stage commit")
        if filename not in self._files:
            raise ContractError(f"stage output was not configured: {filename}")
        line = (
            json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")
        if self._total_bytes + len(line) > self._max_bytes:
            raise ContractError("max_artifact_bytes exceeded")
        self._files[filename].write(line)
        self._digests[filename].update(line)
        self._byte_counts[filename] += len(line)
        self._total_bytes += len(line)

    def part_path(self, filename: str) -> Path:
        """Return the private partial path for a reconstructible external file."""
        self._validate_filename(filename)
        return self._stage_dir / f"{filename}.part"

    def register_external_part(self, filename: str) -> None:
        """Register a closed scratch file to publish with the stage."""
        if filename in self._files or filename in self._external_parts:
            raise ContractError("stage file was already registered")
        path = self.part_path(filename)
        try:
            byte_count = path.stat().st_size
        except OSError as error:
            raise ContractError("registered stage part does not exist") from error
        if self._total_bytes + byte_count > self._max_bytes:
            raise ContractError("max_artifact_bytes exceeded")
        with path.open("rb") as external_file:
            os.fsync(external_file.fileno())
        self._external_parts.add(filename)
        self._byte_counts[filename] = byte_count
        self._digests[filename] = self._repository._file_checksum(path)
        self._total_bytes += byte_count

    def commit(
        self,
        *,
        record_count: int,
        metrics: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactRef:
        """Atomically publish stage files and then their complete manifest."""
        if self._committed:
            raise ContractError("stage writer was already committed")
        self._close_outputs()
        file_metadata: list[tuple[str, str, int]] = []
        for filename in sorted((*self._files, *self._external_parts)):
            digest = self._digests[filename]
            checksum = digest.hexdigest() if hasattr(digest, "hexdigest") else digest
            byte_count = self._byte_counts[filename]
            os.replace(self.part_path(filename), self._stage_dir / filename)
            file_metadata.append((filename, checksum, byte_count))
        self._repository._sync_directory(self._stage_dir)
        artifact_checksum = self._repository._combined_checksum(file_metadata)
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "artifact_type": self._stage,
            "stage": self._stage,
            "complete": True,
            "execution_id": self._execution_id,
            "started_at": self._started_at.isoformat(),
            "completed_at": self._repository.now().isoformat(),
            "input": self._repository._input_payload(self._input_ref),
            "record_count": record_count,
            "metrics": dict(metrics),
            "artifact_checksum": artifact_checksum,
            "files": [
                {
                    "name": filename,
                    "path": self._repository._relative(self._stage_dir / filename),
                    "sha256": checksum,
                    "bytes": byte_count,
                }
                for filename, checksum, byte_count in file_metadata
            ],
        }
        if metadata is not None:
            manifest["metadata"] = dict(metadata)
        manifest_part = self._stage_dir / "manifest.json.part"
        self._repository._write_manifest(manifest_part, manifest)
        manifest_path = self._stage_dir / "manifest.json"
        os.replace(manifest_part, manifest_path)
        self._repository._sync_directory(self._stage_dir)
        self._committed = True
        return ArtifactRef(
            execution_id=self._execution_id,
            stage=self._stage,
            manifest_path=self._repository._relative(manifest_path),
            record_count=record_count,
            checksum=artifact_checksum,
        )

    def _close_outputs(self) -> None:
        for output in self._files.values():
            if not output.closed:
                self._repository._sync_file(output)
                output.close()

    @staticmethod
    def _validate_filename(filename: str) -> None:
        if not filename or Path(filename).name != filename or filename in {".", ".."}:
            raise ValueError("stage filename is unsafe")
