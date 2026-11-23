"""SHA-256 fingerprint of a file in S3 (or any PyArrow filesystem)."""

import hashlib

import pyarrow.fs as pafs

CHUNK_SIZE = 1024 * 1024  # read 1 MB at a time: never load a (possibly huge) file into memory


def sha256_of(fs: pafs.FileSystem, key: str) -> str:
    digest = hashlib.sha256()
    with fs.open_input_stream(key) as f:
        while chunk := f.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()
