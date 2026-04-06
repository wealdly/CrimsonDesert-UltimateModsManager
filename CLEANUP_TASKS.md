# ModsManager — Code Cleanup Tasks

Workspace root: `d:\00-CODE\ModsManager\src\cdumm`

---

## Task 1 — Extract PAZ directory predicate (9 duplicates)

The inline check `d.name.isdigit() and len(d.name) == 4` appears 9+ times across the codebase. A variant `int(d) >= 36` for mod directories also repeats.

**Action:** Create `src/cdumm/archive/paz_format.py`:

```python
def is_paz_dir(name: str) -> bool:
    return name.isdigit() and len(name) == 4

def is_mod_dir(name: str) -> bool:
    return is_paz_dir(name) and int(name) >= 36
```

Replace all occurrences with calls to these functions.

**Files to update:**
- `src/cdumm/gui/main_window.py` lines 57, 358, 446
- `src/cdumm/engine/apply_engine.py` lines 582, 605, 612
- `src/cdumm/engine/import_handler.py` lines 365, 615

---

## Task 2 — Extract PAMT record search pattern (3 duplicates)

`struct.pack("<IIII", entry.offset, entry.comp_size, entry.orig_size, entry.flags)` appears identically in 3 files.

**Action:** Add to `src/cdumm/archive/paz_parse.py`:

```python
import struct

def make_pamt_search_pattern(entry) -> bytes:
    return struct.pack("<IIII", entry.offset, entry.comp_size, entry.orig_size, entry.flags)
```

Replace inline `struct.pack` calls with `make_pamt_search_pattern(entry)`.

**Files to update:**
- `src/cdumm/engine/apply_engine.py` line 288
- `src/cdumm/engine/json_patch_handler.py` line 401
- `src/cdumm/engine/crimson_browser_handler.py` line 336

---

## Task 3 — Consolidate `HASH_INITVAL` constant (3 definitions)

The value `0xC5EDE` is defined three times under different names:
- `hashlittle.py` line 63: `INTEGRITY_SEED = 0xC5EDE`
- `paz_crypto.py` line 20: `HASH_INITVAL = 0x000C5EDE`
- `pathc_handler.py` line 27: `HASH_INITVAL = 0x000C5EDE`

**Action:** Keep `INTEGRITY_SEED` in `hashlittle.py` as the single source of truth. In `paz_crypto.py` and `pathc_handler.py`, replace the local definition with:

```python
from cdumm.archive.hashlittle import INTEGRITY_SEED as HASH_INITVAL
```

No call-site changes needed — the local alias keeps existing code working.

---

## Task 4 — Remove duplicate `hashlittle()` in `paz_crypto.py`

`paz_crypto.py` contains its own full `hashlittle()` implementation (lines ~38–90) that duplicates `hashlittle.py`. `paz_crypto.py` uses it only internally on line 96 for key derivation.

**Action:** In `paz_crypto.py`, delete the local `hashlittle()` function and `_rot`/`_add`/`_sub` helpers, and import instead:

```python
from cdumm.archive.hashlittle import hashlittle
```

Verify line 96 still works: `seed = hashlittle(basename.encode('utf-8'), HASH_INITVAL)`

---

## Task 5 — Move inline `import json` to module level in `delta_engine.py`

`import json` appears inside function bodies at lines 277 and 291 of `delta_engine.py` with no apparent circular dependency reason.

**Action:** Move `import json` to the top-level imports section of `delta_engine.py`. Remove the two inline `import json` statements.

---

## Task 6 — Remove redundant `.replace("/", "\\")` calls (pathlib handles this)

Python's `pathlib.Path` on Windows accepts forward-slash separators natively, so `path / "0008/0.paz"` already produces `path\0008\0.paz`. The `.replace("/", "\\")` calls before joining with a `Path` are redundant.

**Action:** In the following files, for every line matching `some_path / rel_path.replace("/", "\\")`, simplify to `some_path / rel_path`:

- `src/cdumm/engine/apply_engine.py` (~15 occurrences)
- `src/cdumm/gui/main_window.py` (~10 occurrences)
- `src/cdumm/engine/import_handler.py` (~5 occurrences)

**Note:** Only remove `.replace("/", "\\")` when the result is immediately used with `/` to join a `Path`. Do NOT remove it where the string is used directly as a dict key, database value, or printed string — those should stay as POSIX-style paths.

---

## Task 7 — Remove two redundant `import json as _json#` in `main_window.py`

`main_window.py` imports `json` inline with aliases `_json`, `_json2`, `_json3` at lines 2020, 2407, 3206 to avoid shadowing. The standard library `json` has no name collision with anything in the file.

**Action:** Add `import json` to the top-level imports of `main_window.py` and replace all `import json as _json` / `_json.loads` / `_json.dumps` occurrences with `json.loads` / `json.dumps`.

---

## Verification

After all tasks, run:

```
python -m pytest tests/ -q
```

95 tests should pass (6 pre-existing failures in test_database and test_snapshot_manager are unrelated).
