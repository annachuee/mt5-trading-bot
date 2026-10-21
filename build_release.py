"""Create a clean, source-controlled Windows MT5 deployment ZIP bundle."""

from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import os
import subprocess
import tarfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"
BATCH_PATH = ROOT / "run_production.bat"
MINIMUM_HOLDOUT_TRADES = 3

BATCH_CONTENT = r'''@echo off
setlocal
cd /d "%~dp0"

if not exist ".env" (
  echo Missing .env. Copy .env.example to .env and set credentials and risk limits.
  exit /b 2
)

where py >nul 2>nul
if errorlevel 1 (
  echo Python Launcher for Windows was not found. Install 64-bit Python 3.12.
  exit /b 3
)

if not exist ".venv\Scripts\python.exe" py -3.12 -m venv .venv
if errorlevel 1 exit /b 4

".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 5
".venv\Scripts\python.exe" -m pip install -r requirements.txt -r requirements-mt5-windows.txt
if errorlevel 1 exit /b 6

".venv\Scripts\python.exe" mt5_production.py
exit /b %errorlevel%
'''


def _run_git(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def _excluded(name: str) -> bool:
    path = PurePosixPath(name)
    lower = name.lower()
    if path.is_absolute() or ".." in path.parts:
        return True
    if any(part in {".git", ".venv", "venv", "__pycache__", "secrets", "data", "dist", "backtest_results"}
           for part in path.parts):
        return True
    if lower == ".env" or (lower.startswith(".env.") and lower != ".env.example"):
        return True
    if lower.endswith((".log", ".pem", ".key", ".sqlite", ".sqlite3", ".parquet", ".feather")):
        return True
    if fnmatch.fnmatch(path.name.lower(), "* ticks*.csv") or fnmatch.fnmatch(path.name.lower(), "*_ticks.csv"):
        return True
    if "ticks" in path.name.lower() and path.suffix.lower() == ".csv":
        return True
    return False


def _generate_batch() -> None:
    expected = BATCH_CONTENT.encode("utf-8")
    current = BATCH_PATH.read_bytes() if BATCH_PATH.exists() else None
    if current != expected:
        BATCH_PATH.write_bytes(expected)


def _require_clean_tree() -> None:
    status = _run_git("status", "--porcelain", "--untracked-files=all").decode().strip()
    if status:
        raise RuntimeError(
            "Git working tree must be committed before packaging. Remaining changes:\n" + status
        )


def _config_summary() -> dict[str, Any]:
    config = json.loads((ROOT / "config" / "strategy_params.json").read_text(encoding="utf-8"))
    opt = config.get("optimization")
    optimized = isinstance(opt, dict)
    if optimized:
        try:
            optimized = (
                float(opt["holdout_return"]) > 0
                and float(opt["holdout_profit_factor"]) > 1
                and float(opt["holdout_sharpe_ratio"]) > 0
                and int(opt["holdout_closed_trades"]) >= MINIMUM_HOLDOUT_TRADES
                and int(opt.get("minimum_holdout_trades", MINIMUM_HOLDOUT_TRADES))
                    == MINIMUM_HOLDOUT_TRADES
                and opt.get("holdout_qualified") is True
            )
        except (KeyError, TypeError, ValueError):
            optimized = False
    return {
        "strategy_id": config.get("strategy_id"),
        "symbol": config.get("symbol"),
        "fast_period": config.get("fast_period"),
        "slow_period": config.get("slow_period"),
        "risk_fraction": config.get("risk_fraction"),
        "minimum_stop_points": config.get("minimum_stop_points"),
        "adx_period": config.get("adx_period"),
        "adx_lower_threshold": config.get("adx_threshold"),
        "adx_upper_threshold": config.get("adx_max_threshold"),
        "risk_reward_multiple": config.get("risk_reward_multiple"),
        "session_start_utc": config.get("session_start_utc"),
        "session_end_utc": config.get("session_end_utc"),
        "holdout_qualified_optimization": optimized,
    }


def build_release() -> Path:
    _generate_batch()
    _require_clean_tree()
    commit = _run_git("rev-parse", "--short", "HEAD").decode().strip()
    tracked = _run_git("ls-files", "-z").decode().split("\0")
    included = {name for name in tracked if name and not _excluded(name)}
    if "run_production.bat" not in included:
        raise RuntimeError("run_production.bat must be committed before packaging")

    archive = _run_git("archive", "--format=tar", "HEAD")
    DIST.mkdir(parents=True, exist_ok=True)
    output = DIST / f"mt5-platform-release-{commit}.zip"
    manifest = {
        "release_commit": commit,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy_config": _config_summary(),
        "included_file_count": len(included) + 1,
        "excluded_patterns": [
            ".env and environment-specific .env.* files (except .env.example)",
            "*.log", "*_ticks.csv", "local databases and columnar datasets",
            ".venv, secrets, data, backtest_results, dist",
        ],
    }
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as bundle:
            for member in source.getmembers():
                if member.isfile() and member.name in included:
                    content = source.extractfile(member)
                    if content is not None:
                        payload = content.read()
                        if member.name.lower().endswith(".bat"):
                            payload = payload.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
                        bundle.writestr(member.name, payload)
            bundle.writestr("release_manifest.json", json.dumps(manifest, indent=2) + "\n")

    with zipfile.ZipFile(output) as bundle:
        names = bundle.namelist()
        if "run_production.bat" not in names or "mt5_production.py" not in names:
            raise RuntimeError("Release verification failed: production launch files are missing")
        if any(_excluded(name) for name in names):
            raise RuntimeError("Release verification failed: excluded local data found in archive")
    return output


def main() -> int:
    release = build_release()
    digest = hashlib.sha256(release.read_bytes()).hexdigest()
    manifest = json.loads(zipfile.ZipFile(release).read("release_manifest.json"))
    print(f"Release: {release}")
    print(f"Commit: {manifest['release_commit']}")
    print(f"Files: {manifest['included_file_count']} (excluding .env, logs, and tick CSVs)")
    print(f"Strategy: {manifest['strategy_config']}")
    print(f"Zip size: {release.stat().st_size:,} bytes")
    print(f"SHA-256: {digest}")
    if not manifest["strategy_config"]["holdout_qualified_optimization"]:
        print("WARNING: current strategy config is not holdout-qualified; production runner will refuse startup by default.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
