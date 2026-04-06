"""PAZ crypto and compression library.

Provides ChaCha20 encryption/decryption with deterministic key derivation,
and LZ4 block compression/decompression for Crimson Desert PAZ archives.

Keys are derived from the filename alone — no key database needed.

Usage:
    from cdumm.archive.paz_crypto import derive_key_iv, encrypt, decrypt, lz4_compress
"""

import os
import struct

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms
import lz4.block

from cdumm.archive.hashlittle import INTEGRITY_SEED as HASH_INITVAL, hashlittle

# ── Key derivation constants ─────────────────────────────────────────

IV_XOR = 0x60616263
XOR_DELTAS = [
    0x00000000, 0x0A0A0A0A, 0x0C0C0C0C, 0x06060606,
    0x0E0E0E0E, 0x0A0A0A0A, 0x06060606, 0x02020202,
]


# ── Key derivation ───────────────────────────────────────────────────

def derive_key_iv(filename: str) -> tuple[bytes, bytes]:
    """Derive 32-byte ChaCha20 key and 16-byte IV from a filename."""
    basename = os.path.basename(filename).lower()
    seed = hashlittle(basename.encode('utf-8'), HASH_INITVAL)

    iv = struct.pack('<I', seed) * 4
    key_base = seed ^ IV_XOR
    key = b''.join(struct.pack('<I', key_base ^ d) for d in XOR_DELTAS)
    return key, iv


# ── ChaCha20 encrypt/decrypt ────────────────────────────────────────

def chacha20(data: bytes, key: bytes, iv: bytes) -> bytes:
    """ChaCha20 encrypt or decrypt (symmetric)."""
    cipher = Cipher(algorithms.ChaCha20(key, iv), mode=None)
    return cipher.encryptor().update(data)


def decrypt(data: bytes, filename: str) -> bytes:
    """Decrypt data using a key derived from the filename."""
    key, iv = derive_key_iv(filename)
    return chacha20(data, key, iv)


def encrypt(data: bytes, filename: str) -> bytes:
    """Encrypt data using a key derived from the filename (same as decrypt)."""
    return decrypt(data, filename)


# ── LZ4 compression ─────────────────────────────────────────────────

def lz4_decompress(data: bytes, original_size: int) -> bytes:
    """LZ4 block decompression (no frame header)."""
    return lz4.block.decompress(data, uncompressed_size=original_size)


def lz4_compress(data: bytes) -> bytes:
    """LZ4 block compression (no frame header, matching game format)."""
    return lz4.block.compress(data, store_size=False)
