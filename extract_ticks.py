"""Export the last 180 days of raw EURUSD MT5 ticks to CSV.

Run on the Windows host where the active MT5 terminal is installed and logged
in. The MetaTrader5 Python package uses Windows x86-64 wheels and terminal IPC.

    py extract_ticks.py

By default the output is EURUSD_ticks.csv beside this script. Data is written
to a temporary file and atomically moved into place only after at least one
tick has been retrieved successfully.
"""

from __future__ import annotations

import argparse
import csv
import os
import platform
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def extract_ticks(output_path: Path, days: int = 180, chunk_days: int = 1) -> int:
    """Connect to the active MT5 terminal and export raw tick records."""
    if platform.system() != "Windows":
        raise RuntimeError(
            f"MT5 extraction requires the Windows host with an active terminal; "
            f"current host is {platform.system()} ({platform.machine()}). Run this script "
            "on the Windows MT5 machine."
        )
    try:
        import MetaTrader5 as mt5
    except ImportError as exc:
        raise RuntimeError(
            "MetaTrader5 Python package is unavailable. Install the matching Windows "
            "64-bit package on the MT5 host, then rerun."
        ) from exc

    if days < 1 or chunk_days < 1:
        raise ValueError("days and chunk_days must be positive integers")
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    start = datetime.now(timezone.utc) - timedelta(days=days)
    end = datetime.now(timezone.utc)
    temp_name: str | None = None
    total_rows = 0
    fieldnames: tuple[str, ...] | None = None

    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    try:
        terminal = mt5.terminal_info()
        if terminal is None or not bool(getattr(terminal, "connected", False)):
            raise RuntimeError(
                "MT5 terminal is not connected to the trade server. Log in to the "
                "active terminal and rerun."
            )
        if not mt5.symbol_select("EURUSD", True):
            raise RuntimeError(f"Could not select EURUSD: {mt5.last_error()}")

        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", suffix=".csv.tmp",
            prefix="EURUSD_ticks_", dir=output_path.parent, delete=False,
        ) as temp:
            temp_name = temp.name
            writer: csv.DictWriter | None = None
            cursor = start
            chunk = timedelta(days=chunk_days)
            while cursor < end:
                next_cursor = min(cursor + chunk, end)
                # A one-microsecond gap avoids asking adjacent requests for the
                # shared boundary instant while retaining millisecond ticks.
                request_end = min(next_cursor - timedelta(microseconds=1), end)
                ticks = mt5.copy_ticks_range(
                    "EURUSD", cursor, request_end, mt5.COPY_TICKS_ALL
                )
                if ticks is None:
                    raise RuntimeError(
                        f"Tick download failed for {cursor.isoformat()} to "
                        f"{request_end.isoformat()}: {mt5.last_error()}"
                    )
                names = ticks.dtype.names or ()
                if ticks.size:
                    if not names:
                        raise RuntimeError("MT5 returned ticks without named fields")
                    if fieldnames is None:
                        fieldnames = ("timestamp_utc", *names)
                        writer = csv.DictWriter(temp, fieldnames=fieldnames)
                        writer.writeheader()
                    assert writer is not None
                    for tick in ticks:
                        row = {name: _native(tick[name]) for name in names}
                        epoch_ms = int(row.get("time_msc") or int(row.get("time", 0)) * 1000)
                        row["timestamp_utc"] = datetime.fromtimestamp(
                            epoch_ms / 1000, tz=timezone.utc
                        ).isoformat()
                        # DictWriter follows header ordering and ignores insertion order.
                        writer.writerow(row)
                    total_rows += int(ticks.size)
                cursor = next_cursor

        if total_rows == 0 or temp_name is None:
            raise RuntimeError(
                "MT5 returned zero ticks for the requested window. Confirm EURUSD "
                "history is available/downloaded in the terminal."
            )
        if os.path.getsize(temp_name) <= 0:
            raise RuntimeError("Temporary tick export is empty")
        os.replace(temp_name, output_path)
        return total_rows
    except Exception:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
        raise
    finally:
        mt5.shutdown()


def _native(value: Any) -> Any:
    return value.item() if hasattr(value, "item") else value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path,
        default=Path(__file__).resolve().parent / "EURUSD_ticks.csv",
        help="CSV destination (default: EURUSD_ticks.csv beside this script)",
    )
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--chunk-days", type=int, default=1)
    args = parser.parse_args()
    try:
        rows = extract_ticks(args.output, days=args.days, chunk_days=args.chunk_days)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    actual = args.output.expanduser().resolve()
    size = actual.stat().st_size
    # Verify the resulting file contains a header plus data records.
    with actual.open("r", encoding="utf-8", newline="") as stream:
        header = next(csv.reader(stream), None)
        first_data_row = next(csv.reader(stream), None)
    if not header or not first_data_row:
        print(f"ERROR: output file is empty or missing tick rows: {actual}", file=sys.stderr)
        return 1
    print(f"SUCCESS: exported {rows:,} EURUSD ticks ({days_text(args.days)})")
    print(f"File: {actual}")
    print(f"Size: {size:,} bytes; columns: {', '.join(header)}")
    return 0


def days_text(days: int) -> str:
    return f"last {days} days"


if __name__ == "__main__":
    raise SystemExit(main())
