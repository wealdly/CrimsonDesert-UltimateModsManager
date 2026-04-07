"""Mod compatibility validator — checks enabled mods before apply.

Two families of checks:
- V1 / V1b  Game version compatibility (mod built against different game version)
- V3a/b/c   Structural validity (delta files exist, correct format, parseable)
"""
import logging
import struct
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QObject, Signal

from cdumm.archive.paz_parse import PazEntry, make_pamt_search_pattern
from cdumm.engine.delta_engine import ENTRY_MAGIC, FULL_COPY_MAGIC, SPARSE_MAGIC
from cdumm.storage.config import Config
from cdumm.storage.database import Database

logger = logging.getLogger(__name__)

REQUIRED_ENTR_KEYS = frozenset({
    "vanilla_offset", "vanilla_comp_size", "vanilla_orig_size",
    "flags", "paz_index", "pamt_dir", "entry_path",
})


@dataclass
class ValidationIssue:
    severity: str        # "error" | "warning"
    code: str            # "V1" | "V1b" | "V3a" | "V3b" | "V3c"
    check_name: str
    mod_id: int
    mod_name: str
    entry_path: str      # "" for mod-level issues
    description: str
    technical_detail: str


def _read_entry_delta_header(delta_path: Path) -> dict | None:
    """Read only the JSON metadata from an ENTR delta (skips content blob)."""
    try:
        with open(delta_path, "rb") as f:
            magic = f.read(4)
            if magic != ENTRY_MAGIC:
                return None
            meta_len = struct.unpack("<I", f.read(4))[0]
            import json
            return json.loads(f.read(meta_len))
    except Exception:
        return None


def _load_pamt(
    pamt_dir: str,
    vanilla_dir: Path,
    game_dir: Path,
    cache: dict,
) -> bytes | None:
    """Load a vanilla PAMT, trying the backup dir first then the game dir.

    Results are cached by pamt_dir so the same file is only read once.
    """
    if pamt_dir in cache:
        return cache[pamt_dir]

    for base in (vanilla_dir, game_dir):
        candidate = base / pamt_dir / "0.pamt"
        try:
            data = candidate.read_bytes()
            cache[pamt_dir] = data
            return data
        except OSError:
            pass

    cache[pamt_dir] = None
    return None


def validate_enabled_mods(
    db: Database,
    game_dir: Path,
    vanilla_dir: Path,
    progress_cb=None,
) -> list[ValidationIssue]:
    """Check all enabled PAZ mods for compatibility and structural validity.

    Args:
        db: open Database connection
        game_dir: path to the game installation directory
        vanilla_dir: path to the vanilla backup directory
        progress_cb: optional callable(percent: int, msg: str)

    Returns:
        list of ValidationIssue (empty = all mods passed)
    """
    def _progress(pct: int, msg: str) -> None:
        if progress_cb:
            progress_cb(pct, msg)

    issues: list[ValidationIssue] = []
    pamt_cache: dict[str, bytes | None] = {}

    # Phase 0 — setup
    current_fp = Config(db).get("game_version_fingerprint")

    rows = db.connection.execute(
        "SELECT id, name, game_version_hash FROM mods "
        "WHERE enabled=1 AND mod_type='paz' ORDER BY priority"
    ).fetchall()

    if not rows:
        _progress(100, "No enabled PAZ mods to validate")
        return []

    total = len(rows)

    # Phase 1 — per-mod checks
    for mod_idx, (mod_id, mod_name, mod_version_hash) in enumerate(rows):
        base_pct = 10 + int((mod_idx / total) * 85)
        _progress(base_pct, f"Validating {mod_name}…")

        # V1 — game version hash mismatch (mod-level warning)
        version_mismatch = False
        if mod_version_hash and current_fp and mod_version_hash != current_fp:
            version_mismatch = True
            issues.append(ValidationIssue(
                severity="warning",
                code="V1",
                check_name="Game version mismatch",
                mod_id=mod_id,
                mod_name=mod_name,
                entry_path="",
                description=(
                    "This mod was built against a different game version. "
                    "Entry offsets may have shifted, which can cause incorrect "
                    "patching or a game crash."
                ),
                technical_detail=(
                    f"Mod built for:  {mod_version_hash}\n"
                    f"Current game:   {current_fp}"
                ),
            ))

        # Per-delta checks
        delta_rows = db.connection.execute(
            "SELECT file_path, delta_path, entry_path "
            "FROM mod_deltas WHERE mod_id=? ORDER BY file_path",
            (mod_id,),
        ).fetchall()

        for file_path, delta_path_str, db_entry_path in delta_rows:
            display_entry = db_entry_path or file_path
            delta_path = Path(delta_path_str)

            # V3a — file existence
            if not delta_path.exists():
                issues.append(ValidationIssue(
                    severity="error",
                    code="V3a",
                    check_name="Missing delta file",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description="The delta file for this entry is missing from disk.",
                    technical_detail=str(delta_path),
                ))
                continue

            # V3b — magic bytes
            try:
                with open(delta_path, "rb") as f:
                    header = f.read(8)
            except OSError as exc:
                issues.append(ValidationIssue(
                    severity="error",
                    code="V3b",
                    check_name="Unreadable delta file",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description="The delta file could not be read.",
                    technical_detail=str(exc),
                ))
                continue

            magic4 = header[:4]
            valid_magic = (
                magic4 in (ENTRY_MAGIC, SPARSE_MAGIC, FULL_COPY_MAGIC)
                or header == b"BSDIFF40"
            )
            if not valid_magic:
                issues.append(ValidationIssue(
                    severity="error",
                    code="V3b",
                    check_name="Unrecognized delta format",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description=(
                        "The delta file has an unrecognized format and cannot be applied."
                    ),
                    technical_detail=f"First 8 bytes: {header.hex()}",
                ))
                continue

            # Only ENTR deltas need further checks
            if magic4 != ENTRY_MAGIC:
                continue

            # V3c — ENTR metadata validity
            metadata = _read_entry_delta_header(delta_path)
            if metadata is None:
                issues.append(ValidationIssue(
                    severity="error",
                    code="V3c",
                    check_name="Malformed ENTR metadata",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description=(
                        "The entry delta's metadata header could not be parsed."
                    ),
                    technical_detail=str(delta_path),
                ))
                continue

            missing_keys = REQUIRED_ENTR_KEYS - set(metadata.keys())
            if missing_keys:
                issues.append(ValidationIssue(
                    severity="error",
                    code="V3c",
                    check_name="Incomplete ENTR metadata",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description=(
                        "The entry delta is missing required metadata fields and "
                        "cannot be applied."
                    ),
                    technical_detail=f"Missing keys: {', '.join(sorted(missing_keys))}",
                ))
                continue

            # V1b — PAMT entry lookup (only when version mismatch detected)
            if not version_mismatch:
                continue

            pamt_dir = metadata["pamt_dir"]
            pamt_bytes = _load_pamt(pamt_dir, vanilla_dir, game_dir, pamt_cache)

            if pamt_bytes is None:
                issues.append(ValidationIssue(
                    severity="warning",
                    code="V1b",
                    check_name="Cannot verify entry offset",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description=(
                        "Could not load the PAMT to verify this entry's offset "
                        "is still valid for the current game version."
                    ),
                    technical_detail=f"PAMT not found: {pamt_dir}/0.pamt",
                ))
                continue

            entry_obj = PazEntry(
                path=metadata["entry_path"],
                paz_file="",
                offset=metadata["vanilla_offset"],
                comp_size=metadata["vanilla_comp_size"],
                orig_size=metadata["vanilla_orig_size"],
                flags=metadata["flags"],
                paz_index=metadata["paz_index"],
            )
            pattern = make_pamt_search_pattern(entry_obj)
            if pattern not in pamt_bytes:
                issues.append(ValidationIssue(
                    severity="error",
                    code="V1b",
                    check_name="Entry offset no longer valid",
                    mod_id=mod_id,
                    mod_name=mod_name,
                    entry_path=display_entry,
                    description=(
                        "This entry's location in the PAZ archive has changed in "
                        "the current game version. Applying this mod would corrupt "
                        "the game file."
                    ),
                    technical_detail=(
                        f"Expected: offset={metadata['vanilla_offset']}, "
                        f"comp={metadata['vanilla_comp_size']}, "
                        f"orig={metadata['vanilla_orig_size']}, "
                        f"flags=0x{metadata['flags']:08X}\n"
                        f"PAMT: {pamt_dir}/0.pamt"
                    ),
                ))

    _progress(100, f"Done — {len(issues)} issue(s) found")
    return issues


class ValidateWorker(QObject):
    """Background worker that validates enabled mods for compatibility."""

    progress_updated = Signal(int, str)
    finished = Signal(object)     # list[ValidationIssue]
    error_occurred = Signal(str)

    def __init__(self, db_path: Path, game_dir: Path, vanilla_dir: Path) -> None:
        super().__init__()
        self._db_path = db_path
        self._game_dir = game_dir
        self._vanilla_dir = vanilla_dir

    def run(self) -> None:
        try:
            db = Database(self._db_path)
            db.initialize()
            issues = validate_enabled_mods(
                db,
                self._game_dir,
                self._vanilla_dir,
                progress_cb=lambda p, m: self.progress_updated.emit(p, m),
            )
            db.close()
            self.finished.emit(issues)
        except Exception as exc:
            logger.error("Validation failed: %s", exc, exc_info=True)
            self.error_occurred.emit(str(exc))
