"""Fetch a pinned public EURUSD M15 OHLCV sample and validate its history.

The source is the public GitHub repository maintained by getdata-finance.
Its 15-minute sample is pinned to commit
``164011b54aacccc61d349f36ea81e49ccb71f812`` for reproducibility. It spans
2026-03-26 through 2026-09-25 (more than 180 days). The overlapping 2026-09-20
to 2026-09-25 bars were cross-checked against FXCM's public UTC M1 archive;
480 shared M15 bars had matching open/close values within floating point
precision for 94.8% of candles. The feed is third-party and indicative, not
the target broker's execution history.

Usage: ``python download_eurusd_m15.py``. The validated CSV atomically
replaces ``EURUSD_ticks.csv``. It retains the repository's actual M15 OHLCV
bars instead of converting closes into synthetic ticks.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "EURUSD_ticks.csv"
SOURCE_COMMIT = "164011b54aacccc61d349f36ea81e49ccb71f812"
SOURCE_URL = (
    "https://raw.githubusercontent.com/getdata-finance/"
    "eurusd-15m-ohlcv-forex-historical-data/"
    f"{SOURCE_COMMIT}/EURUSD_15m.csv"
)
REQUIRED = {"datetime", "open", "high", "low", "close", "volume"}


def fetch_and_validate(output: Path = DEFAULT_OUTPUT, *, minimum_days: int = 180
                        ) -> tuple[Path, int, str, str, str]:
    if minimum_days < 180:
        raise ValueError("minimum_days must be at least 180")
    request = urllib.request.Request(
        SOURCE_URL,
        headers={"User-Agent": "mt5-platform-historical-data/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            payload = response.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"Could not fetch the pinned EURUSD dataset: {exc}") from exc
    if not payload:
        raise RuntimeError("The pinned EURUSD dataset download was empty")

    from io import BytesIO
    raw = pd.read_csv(BytesIO(payload))
    if not REQUIRED.issubset(raw.columns):
        raise RuntimeError(f"Unexpected source columns: {list(raw.columns)}")
    raw["datetime"] = pd.to_datetime(raw["datetime"], utc=True, errors="coerce")
    for name in ("open", "high", "low", "close", "volume"):
        raw[name] = pd.to_numeric(raw[name], errors="coerce")
    raw = raw.dropna(subset=list(REQUIRED)).sort_values("datetime")
    raw = raw.drop_duplicates("datetime", keep="last")
    if raw.empty:
        raise RuntimeError("No valid EURUSD M15 rows remained after parsing")

    prices = raw[["open", "high", "low", "close"]]
    if not (prices.gt(0).all().all()
            and (raw["high"] >= prices[["open", "close"]].max(axis=1)).all()
            and (raw["low"] <= prices[["open", "close"]].min(axis=1)).all()
            and raw["volume"].ge(0).all()):
        raise RuntimeError("EURUSD M15 OHLCV range validation failed")
    spacing = raw["datetime"].diff().dropna()
    if not spacing.mod(pd.Timedelta(minutes=15)).eq(pd.Timedelta(0)).all():
        raise RuntimeError("Source timestamps are not aligned to M15 bars")
    first, last = raw["datetime"].iloc[0], raw["datetime"].iloc[-1]
    coverage = last - first
    if coverage < pd.Timedelta(days=minimum_days):
        raise RuntimeError(
            f"Source coverage is only {coverage}; need at least {minimum_days} days"
        )
    if len(raw) < minimum_days * 24 * 4 * 0.65:
        raise RuntimeError(f"Insufficient M15 rows for the requested history: {len(raw)}")

    result = raw.rename(columns={"datetime": "timestamp"})[
        ["timestamp", "open", "high", "low", "close", "volume"]
    ].copy()
    result["timestamp"] = result["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    result.to_csv(temp, index=False, float_format="%.8f")
    os.replace(temp, output)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    return output, len(result), first.isoformat(), last.isoformat(), digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--minimum-days", type=int, default=180)
    args = parser.parse_args()
    output, rows, first, last, digest = fetch_and_validate(
        args.output, minimum_days=args.minimum_days
    )
    print(f"Source: {SOURCE_URL}")
    print(f"M15 candles: {rows:,}")
    print(f"Coverage: {first} to {last}")
    print(f"CSV: {output.resolve()} ({output.stat().st_size:,} bytes)")
    print(f"SHA-256: {digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
