"""Shared v2 storage models."""

from __future__ import annotations

from dataclasses import dataclass

from pointline.io.protocols import BronzeFileMetadata


@dataclass(frozen=True)
class ManifestIdentity:
    vendor: str
    data_type: str
    bronze_path: str
    file_hash: str

    @classmethod
    def from_meta(cls, meta: BronzeFileMetadata) -> ManifestIdentity:
        return cls(
            vendor=meta.vendor,
            data_type=meta.data_type,
            bronze_path=meta.bronze_file_path,
            file_hash=meta.sha256,
        )

    def as_tuple(self) -> tuple[str, str, str, str]:
        return (self.vendor, self.data_type, self.bronze_path, self.file_hash)
