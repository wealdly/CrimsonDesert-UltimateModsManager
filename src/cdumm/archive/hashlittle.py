"""hashlittle implementation for PAMT/PAPGT integrity chain.

This is the Bob Jenkins hashlittle hash used by Crimson Desert for:
- PAMT hash: hashlittle(pamt[12:], 0xC5EDE)
- PAPGT hash: hashlittle(papgt[12:], 0xC5EDE)

Port of the C implementation used by the existing PAZ toolchain.
"""
import struct


def hashlittle(data: bytes, initval: int = 0) -> int:
    """Bob Jenkins hashlittle hash function."""
    length = len(data)
    a = b = c = (0xDEADBEEF + length + initval) & 0xFFFFFFFF

    offset = 0
    while length > 12:
        a = (a + struct.unpack_from("<I", data, offset)[0]) & 0xFFFFFFFF
        b = (b + struct.unpack_from("<I", data, offset + 4)[0]) & 0xFFFFFFFF
        c = (c + struct.unpack_from("<I", data, offset + 8)[0]) & 0xFFFFFFFF

        a = (a - c) & 0xFFFFFFFF; a ^= ((c << 4) | (c >> 28)) & 0xFFFFFFFF; c = (c + b) & 0xFFFFFFFF
        b = (b - a) & 0xFFFFFFFF; b ^= ((a << 6) | (a >> 26)) & 0xFFFFFFFF; a = (a + c) & 0xFFFFFFFF
        c = (c - b) & 0xFFFFFFFF; c ^= ((b << 8) | (b >> 24)) & 0xFFFFFFFF; b = (b + a) & 0xFFFFFFFF
        a = (a - c) & 0xFFFFFFFF; a ^= ((c << 16) | (c >> 16)) & 0xFFFFFFFF; c = (c + b) & 0xFFFFFFFF
        b = (b - a) & 0xFFFFFFFF; b ^= ((a << 19) | (a >> 13)) & 0xFFFFFFFF; a = (a + c) & 0xFFFFFFFF
        c = (c - b) & 0xFFFFFFFF; c ^= ((b << 4) | (b >> 28)) & 0xFFFFFFFF; b = (b + a) & 0xFFFFFFFF

        offset += 12
        length -= 12

    # Handle remaining bytes
    remaining = data[offset:]
    if length > 0:
        # Pad to 12 bytes and unpack as three little-endian uint32s.
        # Zero bytes in padded slots contribute nothing, so this is
        # equivalent to the original byte-by-byte conditional accumulation.
        padded = remaining + b"\x00" * (12 - len(remaining))
        va, vb, vc = struct.unpack_from("<III", padded)
        if length >= 1: a = (a + va) & 0xFFFFFFFF
        if length >= 5: b = (b + vb) & 0xFFFFFFFF
        if length >= 9: c = (c + vc) & 0xFFFFFFFF

        # Final mixing
        c ^= b; c = (c - ((b << 14) | (b >> 18))) & 0xFFFFFFFF
        a ^= c; a = (a - ((c << 11) | (c >> 21))) & 0xFFFFFFFF
        b ^= a; b = (b - ((a << 25) | (a >> 7))) & 0xFFFFFFFF
        c ^= b; c = (c - ((b << 16) | (b >> 16))) & 0xFFFFFFFF
        a ^= c; a = (a - ((c << 4) | (c >> 28))) & 0xFFFFFFFF
        b ^= a; b = (b - ((a << 14) | (a >> 18))) & 0xFFFFFFFF
        c ^= b; c = (c - ((b << 24) | (b >> 8))) & 0xFFFFFFFF

    return c


INTEGRITY_SEED = 0xC5EDE


def compute_pamt_hash(pamt_data: bytes) -> int:
    """Compute PAMT integrity hash: hashlittle(pamt[12:], 0xC5EDE)."""
    return hashlittle(pamt_data[12:], INTEGRITY_SEED)


def compute_papgt_hash(papgt_data: bytes) -> int:
    """Compute PAPGT integrity hash: hashlittle(papgt[12:], 0xC5EDE)."""
    return hashlittle(papgt_data[12:], INTEGRITY_SEED)
