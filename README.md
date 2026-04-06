# Crimson Desert Ultimate Mods Manager — wealdly fork

> **This is a personal fork of [faisalkindi/CrimsonDesert-UltimateModsManager](https://github.com/faisalkindi/CrimsonDesert-UltimateModsManager).**
> Full credit and deep thanks to **[@faisalkindi](https://github.com/faisalkindi)** for creating and maintaining the original tool — it is excellent work.
> This fork tracks upstream and adds the changes listed below. No upstream pull requests are planned; changes here may be experimental or opinionated.

---

## Changes in this fork

### Bug fixes
- **Stuck addon removal** — mods importing over existing enabled addons no longer get permanently stuck; the replacement flow correctly disables and re-imports without leaving zombie deltas.
- **Priority after update** — re-importing a mod over itself now restores its original load-order position instead of appending it to the bottom.
- **PAMT CRC on import** — missing or wrong CRC in imported PAMTs is auto-corrected at import time, preventing grey-screen crashes.
- **Multi-queue import** — batch-dropping several mods at once no longer causes the second and later imports to silently fail.
- **Entry-level fallback** — when entry-level PAZ decomposition produces zero changed entries, the engine correctly falls back to byte-level delta instead of recording an empty mod.
- **Configurable source path** — configurable (labeled-JSON) mods now store a correct `source_path` so re-configuration works after a session restart.

### Per-mod undo / direct revert
Each imported mod writes a companion `.undo` file (same sparse SPRS format as `.vranges` backups) storing the vanilla bytes at exactly the positions the mod touches. When you remove a mod, the engine attempts a direct in-place revert without requiring a full Apply cycle. Falls back gracefully to Apply when byte-range conflicts with other enabled mods are detected.

### Loose file mod support
Files under loose game directories (`ui/`, `soundassets/`, `video/`, `fonts/`, `data/`, etc.) are now recognised and handled. "JustLoad"-style zips that ship only loose assets (e.g. `ui/*.mp4` cutscene replacements) import cleanly.

### Loose-file variant picker
Archives or directories containing multiple loose-file mod variants now show a picker dialog — the same treatment PAZ multi-variant zips already got — so you choose exactly which preset to install.

### Auto-detect game directory
On first launch (or after Steam moves your library), the manager auto-detects the game directory via a Steam library scan instead of always falling back to the manual setup dialog. Saved paths are now validated with `validate_game_directory()` instead of a bare `.exists()` check.

### ENTR-aware conflict detection
The conflict detector now compares mods at the PAMT entry level when both mods carry entry-path metadata. Two mods that modify different entries in the same PAZ are correctly reported as "compatible PAZ-level overlap" rather than a hard byte-range conflict.

### Upstream v2.1.2 + v2.1.3 merged
- LZ4 null-padding crash fix (`paz_repack.py`) — XML/CSS files shorter than their vanilla slot are no longer padded with nulls before compression.
- PAMT byte-range delta cleanup — spurious PAMT byte-range deltas created during PAZ entry decomposition are removed after import.

### Auto-update suppressed
The upstream auto-update check is disabled (`_UPDATE_DISABLED = True`) and the version is pinned to `999.0.0` so the fork exe never prompts to overwrite itself with an upstream release.

### Performance improvements
- `SnapshotManager` builds an in-memory path/hash cache on first use — eliminates one SQLite round-trip per file during the hot import path.
- `apply_delta_from_file` now uses a single `open()` call instead of opening the file twice.
- `_try_paz_entry_import` keeps both PAZ files open across the full entry-comparison loop instead of reopening on every iteration (was 200+ `open/close` cycles on 900 MB files).
- `_find_overlapping_delta_groups` issues one batched `IN (…)` query instead of one query per delta.
- Dead-code block (~35 unreachable lines) removed from `snapshot_manager.py`.

---

A desktop mod manager for **Crimson Desert** that handles the game's PAZ/PAMT/PAPGT archive format. Install, manage, and safely combine multiple mods with automatic conflict detection, JSON patch merging, and one-click revert to vanilla.

**Works with Steam.** Xbox Game Pass installations are detected but currently limited by platform restrictions (read-only game files).

![Screenshot](https://raw.githubusercontent.com/faisalkindi/CrimsonDesert-UltimateModsManager/master/screenshots/app.png?v=2)

## Features

### Drag-and-Drop Import
Drop a mod onto the window and it's installed. Supports every mod format in the Crimson Desert modding scene:

| Format | Description |
|--------|-------------|
| `.zip` / `.7z` | Archives containing modified game files or installer scripts |
| Folders | Loose directories with modified PAZ/PAMT files |
| `.json` | JSON byte-patch mods (compatible with [JSON Mod Manager](https://www.nexusmods.com/crimsondesert/mods/113)) |
| `manifest.json` + files | Crimson Browser loose-file mods — automatically repacked into PAZ |
| `.bat` / `.py` | Script-based installers — runs in a visible console, captures changes automatically |
| `.bsdiff` | Pre-generated binary patches (auto-detects target game file) |
| `.asi` | Native ASI plugins (installed to `bin64/`) with bundled ASI Loader |

Batch import supported — drop multiple mods at once.

### JSON Patch Merging
Multiple JSON mods that patch the **same game file** (e.g., Stamina mod + Fat Stacks both editing `iteminfo.pabgb`) are automatically merged at the decompressed content level. Non-overlapping patches from different mods compose perfectly. Overlapping bytes go to the higher-priority mod.

Works for both newly imported mods and mods imported in older versions (fallback three-way merge).

### Entry-Level Script Mod Composition
Script mods (`.bat`) are captured at the PAMT entry level — the manager identifies which individual game files changed inside each PAZ archive and stores the decompressed content. This means two script mods modifying different files in the same PAZ compose correctly instead of corrupting each other.

### Delta-Based Patching
Mods are stored as binary deltas against vanilla game files, not full file copies:

- **Small on disk** — only the changed bytes are saved
- **Composable** — multiple mods can modify the same PAZ file at different offsets
- **Reversible** — vanilla files are always preserved and restorable

The engine automatically selects between sparse patches (small, scattered changes), entry-level deltas (decompressed game files), and bsdiff4 (large modifications).

### 3-Level Conflict Detection

When two mods touch the same files, the manager detects the conflict and shows exactly what overlaps:

| Level | What It Means | Action |
|-------|--------------|--------|
| **PAPGT** (metadata) | Mods modify PAMT in different directories | Auto-handled, no action needed |
| **PAZ** (archive) | Same PAZ file, different byte ranges | Usually compatible — shown as info |
| **Byte-Range** (data) | Overlapping byte ranges in the same file | Resolved by load order — winner shown in UI |

**Dangerous overlaps are shown as a blocking warning before Apply** — lists every conflict pair and which mod wins.

### Trust & Transparency
- **Apply Preview** — see exactly what files will be changed before modifying anything
- **Verify Game State** — scan all files and see what's vanilla vs modded
- **Activity Log** — persistent, color-coded history of every action across sessions
- **Post-apply verification** — confirms PAPGT/PAMT integrity after every Apply

### Load Order & Priority
- Drag mods up and down to set priority
- Higher position = applied last = wins conflicts
- Enable/disable individual mods without removing them
- **Export/Import Mod List** — save and restore your entire setup (enabled state, load order, priorities)

### One-Click Apply & Revert
- **Apply** composes all enabled mods onto vanilla files in correct dependency order (PAZ first, then PAMT, then PAPGT)
- **Revert to Vanilla** restores original game files from full vanilla backups
- Crash recovery via `.pre-apply` markers if something goes wrong mid-apply

### Game Update Detection
- Detects game updates and hotfixes automatically (via Steam build ID + exe hash)
- Warns about mods imported for a different game version
- One-time migration on major updates — guides you through verify + rescan

### Script Mod Support
For mods distributed as installer scripts (`.bat` or `.py`):

1. Drop the zip/script onto the manager
2. A console window opens — interact with the installer normally
3. The manager parses the PAMT to identify which game files changed, extracts and decompresses each entry, and stores the decompressed content
4. The mod is now managed like any other — can be disabled, reordered, or reverted

The manager passes `CDMM_GAME_DIR` as an environment variable so scripts can find the game directory automatically.

### Mod Health Check
Every mod is automatically validated before import:

- **Duplicate PAMT paths** — detects overlay mods that add files already in another PAZ directory and handles them correctly (skips PAPGT entry to avoid crashes)
- **Hash mismatches** — verifies PAMT and PAPGT integrity chains
- **PAZ size errors** — catches when PAMT size fields don't match actual files
- **Version mismatches** — warns if the mod was built for a different game version

### Find Problem Mod (Delta Debugging)
When a combination of mods crashes the game, the **Find Problem Mod** wizard uses the Delta Debugging algorithm (ddmin) to find the minimal set of mods causing the crash:

- Tests subsets of your enabled mods automatically
- You launch the game and report crash/no-crash after each test
- Finds single bad mods, conflict pairs, and multi-mod interactions
- Progress is saved — you can resume later if interrupted
- Typically finds the culprit in 10-20 rounds

### ASI Plugin Management
A dedicated **ASI Plugins** tab for managing native DLL plugins:

- Scans `bin64/` for installed `.asi` files
- **Bundled ASI Loader** — auto-installs `winmm.dll` if missing
- Install, update, uninstall, enable/disable plugins
- Opens `.ini` config files in your text editor
- Detects ASI Loader variants (winmm.dll, version.dll, dinput8.dll, dsound.dll)

### Configurable Mods
JSON mods with labeled presets (e.g., "x5 loot", "x10 loot", "x99 loot") show a toggle picker during import. Choose which variant you want. Configurable mods display a gear icon in the mod list.

### Vanilla Snapshot
On first launch, the manager takes a SHA-256 snapshot of all game files. This snapshot is used for:

- Detecting changes made by script mods
- Generating accurate deltas
- Verifying game file integrity
- Blocking snapshots on modded files (prevents dirty backups)

### Bug Report
Built-in diagnostic report generator that collects system info, installed mods, conflict status, database state, and recent log entries. Copy to clipboard or save as a file.

## Installation

### Option 1: Standalone Executable (Recommended)
Download `CDUMM.exe` from the [Releases](https://github.com/wealdly/CrimsonDesert-UltimateModsManager/releases) page of this fork, or from the [upstream releases](https://github.com/faisalkindi/CrimsonDesert-UltimateModsManager/releases). No Python required. Just run it.

### Option 2: Run from Source (this fork)
Requires Python 3.10+.

```bash
git clone https://github.com/wealdly/CrimsonDesert-UltimateModsManager.git
cd CrimsonDesert-UltimateModsManager
pip install -e .
py -3 -m cdumm.main
```

### Building the Executable

```bash
pip install pyinstaller
pyinstaller cdumm.spec --noconfirm
```

The exe is written to `dist/CDUMM.exe`.

## How It Works

Crimson Desert stores game data in PAZ archives, indexed by PAMT files, with PAPGT as a hash registry. This manager:

1. **Snapshots** vanilla game files on first run
2. **Imports** mods by diffing modified files against vanilla, storing only the binary delta (or decompressed entry content for script/JSON mods)
3. **Merges** JSON patches from multiple mods at the decompressed content level (three-way merge against vanilla)
4. **Composes** all enabled mod deltas onto vanilla in priority order when you click Apply (PAZ first, then PAMT, then PAPGT)
5. **Rebuilds** the PAPGT integrity chain so the game accepts the modified files
6. **Commits** atomically — all files are staged and swapped in one operation

Mod data is stored in `<GameDir>/CDMods/`:
- `vanilla/` — full backups of PAMT files, byte-range backups for PAZ files
- `deltas/` — binary patches and entry-level deltas for each mod

App config is stored in `%LOCALAPPDATA%\cdumm\cdumm.db`.

## Requirements

- Windows 10/11
- Crimson Desert (Steam version recommended, Xbox Game Pass detected but limited)

## Support

If you find this useful, consider supporting development:

[![Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/kindiboy)

## Credits

- **Lazorr** — [Crimson Desert Unpacker](https://www.nexusmods.com/crimsondesert/mods/62) — PAZ parsing and repacking tools that CDUMM's archive pipeline is built on
- **PhorgeForge** — [JSON Mod Manager](https://www.nexusmods.com/crimsondesert/mods/113) — JSON byte-patch mod format, natively supported by CDUMM
- **993499094** — [Crimson Desert QT Mod Manager](https://www.nexusmods.com/crimsondesert/mods/218) — Hard link deployment approach and modinfo.json format
- **callmeslinkycd** — [Crimson Desert PATHC Tool](https://www.nexusmods.com/crimsondesert/mods/396) — PATHC texture index parser and repacker that CDUMM's DDS texture mod support is built on

## License

MIT
