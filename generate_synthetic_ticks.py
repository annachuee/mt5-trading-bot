"""Generate deterministic synthetic EURUSD ticks for offline sandboxing.

This is mock GBM data, not broker history and not suitable for evaluating live
strategy performance. Default output is EURUSD_ticks.csv beside this script.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate(
    output: Path,
    *,
    rows: int = 100_000,
    seed: int = 20261020,
    start_price: float = 1.08500,
    annual_volatility: float = 0.10,
    mean_interarrival_seconds: float = 0.5,
    spread: float = 0.00008,
    price_tick: float = 0.00001,
) -> dict[str, float | int | str]:
    """Write UTC timestamp, bid, ask, volume rows from a GBM price path."""
    if rows < 1:
        raise ValueError("rows must be positive")
    if start_price <= 0 or annual_volatility <= 0 or mean_interarrival_seconds <= 0:
        raise ValueError("price, volatility, and inter-arrival interval must be positive")
    if spread <= 0 or price_tick <= 0:
        raise ValueError("spread and price_tick must be positive")

    rng = random.Random(seed)
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    price = start_price
    half_spread = spread / 2.0
    annual_seconds = 365.25 * 24 * 60 * 60
    first_time = timestamp
    min_bid = float("inf")
    max_ask = 0.0

    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("timestamp", "bid", "ask", "volume"))
        for _ in range(rows):
            if _:
                dt = rng.expovariate(1.0 / mean_interarrival_seconds)
                timestamp += timedelta(seconds=dt)
                # GBM: dS/S = mu*dt + sigma*sqrt(dt)*Z. Set annual drift to
                # zero to avoid injecting a directional bias into the sandbox.
                dt_years = dt / annual_seconds
                price *= math.exp(
                    -0.5 * annual_volatility**2 * dt_years
                    + annual_volatility * math.sqrt(dt_years) * rng.gauss(0.0, 1.0)
                )

            bid = math.floor((price - half_spread) / price_tick) * price_tick
            ask = math.ceil((price + half_spread) / price_tick) * price_tick
            bid = round(bid, 5)
            ask = round(ask, 5)
            volume = rng.randint(1, 100)
            writer.writerow((timestamp.isoformat(timespec="milliseconds"),
                             f"{bid:.5f}", f"{ask:.5f}", volume))
            min_bid = min(min_bid, bid)
            max_ask = max(max_ask, ask)

    return {
        "path": str(output),
        "rows": rows,
        "seed": seed,
        "start_timestamp_utc": first_time.isoformat(),
        "end_timestamp_utc": timestamp.isoformat(timespec="milliseconds"),
        "min_bid": min_bid,
        "max_ask": max_ask,
        "size_bytes": output.stat().st_size,
    }


def main() -> int:
    default_path = Path(__file__).resolve().parent / "EURUSD_ticks.csv"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=default_path)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=20261020)
    parser.add_argument("--start-price", type=float, default=1.08500)
    parser.add_argument("--annual-volatility", type=float, default=0.10)
    parser.add_argument("--mean-interarrival-seconds", type=float, default=0.5)
    parser.add_argument("--spread", type=float, default=0.00008)
    args = parser.parse_args()

    result = generate(
        args.output,
        rows=args.rows,
        seed=args.seed,
        start_price=args.start_price,
        annual_volatility=args.annual_volatility,
        mean_interarrival_seconds=args.mean_interarrival_seconds,
        spread=args.spread,
    )
    if result["rows"] != args.rows or result["size_bytes"] <= 0:
        raise RuntimeError("Synthetic tick file verification failed")
    print(f"Created {result['rows']:,} synthetic ticks: {result['path']}")
    print(f"UTC range: {result['start_timestamp_utc']} to {result['end_timestamp_utc']}")
    print(f"Bid/ask range: {result['min_bid']:.5f} to {result['max_ask']:.5f}")
    print(f"Size: {result['size_bytes']:,} bytes")
    print("Data type: synthetic GBM sandbox data; not real MT5 history")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
